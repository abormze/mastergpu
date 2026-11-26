package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gpupanel/internal/gpu"
	"gpupanel/internal/lb"
	"gpupanel/internal/models"
	"gpupanel/internal/store"
	"gpupanel/internal/sysmon"
)

type State string

const (
	Stopped    State = "stopped"
	Starting   State = "starting"
	Running    State = "running"
	Restarting State = "restarting"
	Failed     State = "error"
	Waiting    State = "waiting" // on-demand / scheduled
)

type Status struct {
	ID        int64     `json:"id"`
	State     State     `json:"state"`
	PID       int       `json:"pid"`
	Since     int64     `json:"since"`
	Uptime    int64     `json:"uptime"`
	Input     int       `json:"input"`
	InputURL  string    `json:"input_url"`
	GPU       int       `json:"gpu"`
	FPS       float64   `json:"fps"`
	Speed     float64   `json:"speed"`
	Bitrate   float64   `json:"bitrate"` // kbit/s
	Frames    int64     `json:"frames"`
	Drops     int64     `json:"drops"`
	Dups      int64     `json:"dups"`
	Restarts  int       `json:"restarts"`
	LastError string    `json:"last_error"`
	Viewers   int       `json:"viewers"`
	CPU       float64   `json:"cpu"`
	RSS       uint64    `json:"rss"`
	Variants  []Variant `json:"variants,omitempty"`
	Video     string    `json:"video"`
	Audio     string    `json:"audio"`
	Black     bool      `json:"black"`
	Silent    bool      `json:"silent"`
	Quality   float64   `json:"quality"`
	InputBuf  int64     `json:"input_buf"` // ms held by the input pacer
}

type proc struct {
	mu       sync.Mutex
	st       Status
	cancel   context.CancelFunc
	cmd      *exec.Cmd
	want     bool
	hub      *Hub
	ohubs    map[int]*Hub
	last     time.Time // last progress
	lowSince time.Time
	ticks    uint64
	tickT    time.Time
	done     chan struct{}
	manual   int      // forced input index (-1 none)
	infoIn   []string // ffmpeg's input/output stream dump
	infoOut  []string
	dumpOut  bool
	// low latency: source connection kept open across ffmpeg restarts
	src       *splicer
	srcKey    string
	srcCancel context.CancelFunc
	srcTimer  *time.Timer
}

// source returns the kept-open splicer for key, opening a new one if needed.
func (p *proc) source(key string, mk func() *splicer) *splicer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.srcTimer != nil {
		p.srcTimer.Stop()
		p.srcTimer = nil
	}
	if p.src != nil && p.srcKey == key && p.src.alive() {
		return p.src
	}
	if p.srcCancel != nil {
		p.srcCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.src, p.srcKey, p.srcCancel = mk(), key, cancel
	go p.src.serve(ctx)
	return p.src
}

func (p *proc) srcNow() *splicer {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.src
}

// releaseSource closes the kept-open source after linger unless reattached.
func (p *proc) releaseSource(linger time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.src == nil {
		return
	}
	if p.srcTimer != nil {
		p.srcTimer.Stop()
	}
	src, cancel := p.src, p.srcCancel
	close := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.src == src {
			cancel()
			p.src, p.srcCancel, p.srcTimer = nil, nil, nil
		}
	}
	if linger <= 0 {
		p.srcTimer = nil
		go close()
		return
	}
	p.srcTimer = time.AfterFunc(linger, close)
}

type Hooks struct {
	OnState func(id int64, name string, from, to State, msg string)
}

type Manager struct {
	st      *store.Store
	gm      *gpu.Monitor
	mu      sync.Mutex
	procs   map[int64]*proc
	Hooks   Hooks
	ffVer   int
	Gateway string
	PrepDRM func(ctx context.Context, s *models.Stream, input, root string) (*BuildOpts, error)
	Relay   func(s *models.Stream) string // returns local http proxy url when stream needs proxy/bind/dns
}

func NewManager(st *store.Store, gm *gpu.Monitor) *Manager {
	m := &Manager{st: st, gm: gm, procs: map[int64]*proc{}}
	m.ffVer = FFmpegMajor(m.settings().FFmpegPath)
	return m
}

func (m *Manager) settings() models.Settings {
	s := models.DefaultSettings()
	_ = m.st.Get(models.KSettings, 1, &s)
	return s
}

func (m *Manager) FFVersion() int { return m.ffVer }

var verRe = regexp.MustCompile(`ffmpeg version n?(\d+)`)

func FFmpegMajor(bin string) int {
	out, err := exec.Command(bin, "-version").Output()
	if err != nil {
		return 4
	}
	if mm := verRe.FindSubmatch(out); mm != nil {
		v, _ := strconv.Atoi(string(mm[1]))
		return v
	}
	return 7 // git builds
}

func (m *Manager) get(id int64) *proc {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.procs[id]
	if p == nil {
		p = &proc{st: Status{ID: id, State: Stopped, GPU: -1}, hub: NewHub(), manual: -1}
		m.procs[id] = p
	}
	return p
}

func (m *Manager) Hub(id int64) *Hub { return m.get(id).hub }

// OutHub returns the MPEG-TS hub of HTTP output n (1-based) of a stream.
func (m *Manager) OutHub(id int64, n int) *Hub { return m.get(id).outHub(n) }

func (p *proc) outHub(n int) *Hub {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ohubs == nil {
		p.ohubs = map[int]*Hub{}
	}
	h := p.ohubs[n]
	if h == nil {
		h = NewHub()
		p.ohubs[n] = h
	}
	return h
}

func (m *Manager) Status(id int64) Status {
	p := m.get(id)
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.st
	if s.State == Running {
		s.Uptime = time.Now().Unix() - s.Since
	}
	s.Viewers = p.hub.Count() + viewerCount(id)
	for _, h := range p.ohubs {
		s.Viewers += h.Count()
	}
	return s
}

func (m *Manager) All() []Status {
	m.mu.Lock()
	ids := make([]int64, 0, len(m.procs))
	for id := range m.procs {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]Status, 0, len(ids))
	for _, id := range ids {
		out = append(out, m.Status(id))
	}
	return out
}

// GPUAssigned counts running streams per GPU index.
func (m *Manager) GPUAssigned() map[int]int {
	r := map[int]int{}
	for _, s := range m.All() {
		if s.State == Running || s.State == Starting {
			r[s.GPU]++
		}
	}
	return r
}

func (m *Manager) Running(id int64) bool {
	p := m.get(id)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.want
}

func (m *Manager) Start(id int64) error {
	var s models.Stream
	if err := m.st.Get(models.KStream, id, &s); err != nil {
		return err
	}
	p := m.get(id)
	p.mu.Lock()
	if p.want {
		p.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.want, p.cancel, p.done = true, cancel, make(chan struct{})
	p.st.Restarts = 0
	p.mu.Unlock()
	go m.loop(ctx, id, p)
	return nil
}

func (m *Manager) Stop(id int64) {
	p := m.get(id)
	p.mu.Lock()
	if !p.want {
		p.mu.Unlock()
		return
	}
	p.want = false
	cancel, done := p.cancel, p.done
	p.mu.Unlock()
	cancel()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
	}
}

func (m *Manager) Restart(id int64) error {
	m.Stop(id)
	return m.Start(id)
}

// SwitchInput forces a specific source index and restarts.
func (m *Manager) SwitchInput(id int64, idx int) error {
	p := m.get(id)
	p.mu.Lock()
	p.manual = idx
	p.mu.Unlock()
	return m.Restart(id)
}

func (m *Manager) StopAll() {
	var wg sync.WaitGroup
	for _, s := range m.All() {
		wg.Add(1)
		go func(id int64) { defer wg.Done(); m.Stop(id) }(s.ID)
	}
	wg.Wait()
}

func (m *Manager) Forget(id int64) {
	m.Stop(id)
	m.mu.Lock()
	delete(m.procs, id)
	m.mu.Unlock()
	os.RemoveAll(filepath.Join(m.settings().HLSDir, strconv.FormatInt(id, 10)))
}

func (m *Manager) setState(id int64, name string, p *proc, to State, msg string) {
	p.mu.Lock()
	from := p.st.State
	p.st.State = to
	if msg != "" {
		p.st.LastError = msg
	}
	if to == Running {
		p.st.Since = time.Now().Unix()
	}
	p.mu.Unlock()
	if from != to && m.Hooks.OnState != nil {
		m.Hooks.OnState(id, name, from, to, msg)
	}
}

func (m *Manager) loop(ctx context.Context, id int64, p *proc) {
	defer close(p.done)
	defer p.releaseSource(20 * time.Second)
	fails := 0
	for {
		var s models.Stream
		if err := m.st.Get(models.KStream, id, &s); err != nil {
			m.setState(id, "", p, Stopped, "stream deleted")
			return
		}
		st := m.settings()
		if s.ProfileID > 0 {
			var pr models.Profile
			if m.st.Get(models.KProfile, s.ProfileID, &pr) == nil {
				s.Transcode = MergeProfile(pr.Transcode, s.Transcode)
				if pr.GPU != nil && *pr.GPU != -3 && (s.GPU == -2 || s.GPU == -3) {
					s.GPU = *pr.GPU
				}
				if s.ServerID == 0 {
					s.ServerID = pr.ServerID
				}
			}
		}
		s.Inputs = EnabledInputs(s.Inputs)
		if len(s.Inputs) == 0 && !s.DRM.Enabled {
			m.setState(id, s.Name, p, Failed, "no source URL")
			return
		}
		// pick input
		p.mu.Lock()
		idx := p.st.Input
		if p.manual >= 0 {
			idx, p.manual = p.manual, -1
		}
		p.mu.Unlock()
		if len(s.Inputs) > 0 {
			idx = ((idx % len(s.Inputs)) + len(s.Inputs)) % len(s.Inputs)
		}
		// pick gpu
		g := m.pickGPU(&s, st)
		if g == -2 {
			m.setState(id, s.Name, p, Waiting, "no GPU capacity (load balancer)")
			if !sleepCtx(ctx, 10*time.Second) {
				m.setState(id, s.Name, p, Stopped, "")
				return
			}
			continue
		}
		root := filepath.Join(st.HLSDir, strconv.FormatInt(id, 10))
		os.RemoveAll(root)
		os.MkdirAll(root, 0o755)
		opts := BuildOpts{GPU: g, HLSRoot: root, HTTPPipe: true, FFmpegVer: m.ffVer, Gateway: m.Gateway}
		if len(s.Inputs) > 0 {
			opts.Input = s.Inputs[idx]
		}
		runCtx, runCancel := context.WithCancel(ctx)
		if s.DRM.Enabled && m.PrepDRM != nil {
			o2, err := m.PrepDRM(runCtx, &s, opts.Input, root)
			if err != nil {
				runCancel()
				m.st.Log("stream", id, "error", "DRM: "+err.Error())
				m.setState(id, s.Name, p, Restarting, "DRM: "+err.Error())
				fails++
				if !sleepCtx(ctx, backoff(st.RestartDelay, fails)) {
					m.setState(id, s.Name, p, Stopped, "")
					return
				}
				continue
			}
			opts.Input, opts.Inputs2, opts.InputKeys = o2.Input, o2.Inputs2, o2.InputKeys
			opts.CENC, opts.AudioInput, opts.Proxy, opts.CopyTS = o2.CENC, o2.AudioInput, o2.Proxy, o2.CopyTS
		} else if m.Relay != nil {
			opts.Proxy = m.Relay(&s)
		}
		src := opts.Input
		var spl *splicer
		if !s.DRM.Enabled && !st.InputDirect && spliceable(opts.Input) {
			ua := s.UserAgent
			if ua == "" {
				ua = st.UserAgent
			}
			mk := func() *splicer {
				sp := newSplicer(opts.Input, ua, s.Referer, s.Headers, opts.Proxy, time.Duration(st.StreamTimeout)*time.Second/2,
					func(msg string) { m.st.Log("stream", id, "info", msg) })
				sp.trim = true
				return sp
			}
			spl = p.source(strings.Join([]string{opts.Input, ua, s.Referer, s.Headers, opts.Proxy}, "\x00"), mk)
			opts.Input = "pipe:0"
		}
		if spl == nil {
			p.releaseSource(0)
		}
		args, variants, err := Build(&s, &st, opts)
		if err != nil {
			runCancel()
			m.setState(id, s.Name, p, Failed, err.Error())
			p.mu.Lock()
			p.want = false
			p.mu.Unlock()
			return
		}
		for _, v := range variants {
			os.MkdirAll(filepath.Join(root, v.Dir), 0o755)
		}
		for oi, o := range s.Outputs {
			if o.Type == "dash" || (o.Type == "hls" && !o.Disabled) {
				os.MkdirAll(filepath.Join(root, def(o.Name, "o"+strconv.Itoa(oi))), 0o755)
			}
		}
		writeMaster(root, variants)
		p.mu.Lock()
		p.st.Input, p.st.InputURL, p.st.GPU, p.st.Variants = idx, redact(src), g, variants
		p.st.FPS, p.st.Speed, p.st.Bitrate, p.st.Frames = 0, 0, 0, 0
		p.mu.Unlock()
		m.setState(id, s.Name, p, Starting, "")
		m.st.Log("stream", id, "info", fmt.Sprintf("start input #%d gpu=%d", idx+1, g))
		started := time.Now()
		reason := m.run(runCtx, id, &s, &st, p, args, spl)
		runCancel()
		if ctx.Err() != nil {
			m.setState(id, s.Name, p, Stopped, "")
			p.hub.Reset()
			return
		}
		p.mu.Lock()
		p.st.Restarts++
		p.mu.Unlock()
		m.st.Log("stream", id, "warn", "exit: "+reason)
		if time.Since(started) < 30*time.Second {
			fails++
		} else {
			fails = 0
		}
		// fallback: move to next source after a failure
		if len(s.Inputs) > 1 && (fails > 0 || strings.Contains(reason, "stall")) {
			p.mu.Lock()
			p.st.Input = (idx + 1) % len(s.Inputs)
			p.mu.Unlock()
			m.st.Log("stream", id, "warn", fmt.Sprintf("switch to source #%d", (idx+1)%len(s.Inputs)+1))
		}
		m.setState(id, s.Name, p, Restarting, reason)
		if !sleepCtx(ctx, backoff(st.RestartDelay, fails)) {
			m.setState(id, s.Name, p, Stopped, "")
			return
		}
	}
}

func backoff(base, fails int) time.Duration {
	if base <= 0 {
		base = 3
	}
	d := time.Duration(base) * time.Second
	for i := 1; i < fails && d < 60*time.Second; i++ {
		d *= 2
	}
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func (m *Manager) pickGPU(s *models.Stream, st models.Settings) int {
	t := s.Transcode
	if isCopy(&t) || t.HWAccel == "" || t.HWAccel == "none" || t.HWAccel == "qsv" {
		return -1
	}
	if s.GPU >= 0 {
		return s.GPU
	}
	info := m.gm.Get()
	if !info.Available {
		if st.LBCPUFallback {
			return -1
		}
		return -2
	}
	l := lb.Limits{Strategy: st.LBMode, MaxEnc: float64(st.LBMaxGPUEnc), MaxMem: float64(st.LBMaxGPUMem), MaxTemp: float64(st.AlertGPUTemp + 2), CPUFallback: st.LBCPUFallback}
	if s.BalancerID > 0 {
		var b models.Balancer
		if m.st.Get(models.KBalancer, s.BalancerID, &b) == nil && b.Enabled {
			l = lb.Limits{Strategy: b.Strategy, MaxEnc: float64(b.MaxEnc), MaxMem: float64(b.MaxMem), MaxTemp: float64(b.MaxTemp), MaxPerGPU: b.MaxPerGPU, CPUFallback: b.CPUFallbk}
		}
	}
	return lb.PickGPU(info.Cards, m.GPUAssigned(), l)
}

var progRe = regexp.MustCompile(`^(\w+)=(.*)$`)

func (m *Manager) run(ctx context.Context, id int64, s *models.Stream, st *models.Settings, p *proc, args []string, spl *splicer) string {
	cmd := exec.Command(st.FFmpegPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	var ir, iw *os.File
	if spl != nil {
		var err error
		if ir, iw, err = os.Pipe(); err != nil {
			return err.Error()
		}
		cmd.Stdin = ir
	}
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	pr, pw, err := os.Pipe()
	if err != nil {
		return err.Error()
	}
	cmd.ExtraFiles = []*os.File{pw}
	var ors []*os.File
	var ohubs []*Hub
	for _, n := range HTTPOutputs(s) {
		r2, w2, err := os.Pipe()
		if err != nil {
			break
		}
		ors, ohubs = append(ors, r2), append(ohubs, p.outHub(n))
		cmd.ExtraFiles = append(cmd.ExtraFiles, w2)
	}
	closeW := func() {
		for _, f := range cmd.ExtraFiles {
			f.Close()
		}
	}
	if err := cmd.Start(); err != nil {
		if ir != nil {
			ir.Close()
			iw.Close()
		}
		pr.Close()
		closeW()
		for _, f := range ors {
			f.Close()
		}
		return "spawn: " + err.Error()
	}
	closeW()
	if ir != nil {
		ir.Close()
		go func() {
			if spl.dead != nil && spl.trim && spl == p.srcNow() {
				spl.attach(ctx, iw)
			} else {
				spl.run(ctx, iw)
			}
			iw.Close()
		}()
	}
	go p.hub.Pump(pr)
	for i, f := range ors {
		h := ohubs[i]
		h.Reset()
		go func(f *os.File) { h.Pump(f); f.Close() }(f)
	}
	p.mu.Lock()
	p.cmd = cmd
	p.st.PID = cmd.Process.Pid
	p.last = time.Now()
	p.lowSince = time.Time{}
	p.mu.Unlock()

	errLines := make(chan string, 64)
	var tail []string
	var tmu sync.Mutex
	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			l := sc.Text()
			if i := strings.Index(l, "[info] "); i >= 0 {
				p.mu.Lock()
				dumpLine(p, l[i+7:])
				p.mu.Unlock()
				continue
			}
			for _, t := range []string{"[warning] ", "[error] ", "[fatal] ", "[panic] "} {
				l = strings.Replace(l, t, "", 1)
			}
			tmu.Lock()
			tail = append(tail, l)
			if len(tail) > 30 {
				tail = tail[len(tail)-30:]
			}
			tmu.Unlock()
			select {
			case errLines <- l:
			default:
			}
		}
	}()
	go func() {
		seen, n, drop, win := map[string]bool{}, 0, 0, time.Now()
		for l := range errLines {
			if isNoise(l) {
				continue
			}
			if time.Since(win) > time.Minute {
				if drop > 0 {
					m.st.Log("stream", id, "ffmpeg", fmt.Sprintf("… %d repeated lines suppressed", drop))
				}
				clear(seen)
				n, drop, win = 0, 0, time.Now()
			}
			k := hexAddr.ReplaceAllString(l, "")
			if seen[k] || n >= 30 {
				drop++
				continue
			}
			seen[k] = true
			n++
			m.st.Log("stream", id, "ffmpeg", redact(l))
		}
	}()
	go func() {
		sc := bufio.NewScanner(stdout)
		running := false
		var wrote int64
		var wroteAt time.Time
		// ffmpeg's speed/fps are averages since start; show the last ~30s instead
		type sample struct {
			t      time.Time
			ot, fr int64
		}
		var win []sample
		var ot int64 = -1
		for sc.Scan() {
			mm := progRe.FindStringSubmatch(sc.Text())
			if mm == nil {
				continue
			}
			k, v := mm[1], strings.TrimSpace(mm[2])
			p.mu.Lock()
			switch k {
			case "frame":
				n, _ := strconv.ParseInt(v, 10, 64)
				if n > p.st.Frames {
					p.last = time.Now()
				}
				p.st.Frames = n
			case "fps":
				if len(win) < 2 {
					p.st.FPS, _ = strconv.ParseFloat(v, 64)
				}
			case "total_size", "out_time_us":
				if k == "out_time_us" {
					if n, err := strconv.ParseInt(v, 10, 64); err == nil {
						ot = n
					}
				}
				if isCopy(&s.Transcode) {
					p.last = time.Now()
				}
			case "drop_frames":
				p.st.Drops, _ = strconv.ParseInt(v, 10, 64)
			case "dup_frames":
				p.st.Dups, _ = strconv.ParseInt(v, 10, 64)
			case "stream_0_0_q":
				p.st.Quality, _ = strconv.ParseFloat(v, 64)
			case "speed":
				if len(win) < 2 {
					p.st.Speed, _ = strconv.ParseFloat(strings.TrimSuffix(v, "x"), 64)
				}
			case "progress":
				if spl != nil {
					p.st.InputBuf = spl.buffered()
				}
				if ot >= 0 {
					now := time.Now()
					win = append(win, sample{now, ot, p.st.Frames})
					for len(win) > 2 && now.Sub(win[0].t) > 30*time.Second {
						win = win[1:]
					}
					if a, b := win[0], win[len(win)-1]; len(win) >= 2 {
						if dt := b.t.Sub(a.t).Seconds(); dt > 0 {
							p.st.Speed = math.Round(float64(b.ot-a.ot)/1e6/dt*1000) / 1000
							p.st.FPS = math.Round(float64(b.fr-a.fr)/dt*100) / 100
						}
					}
				}
				// stream copy reports frame=0; show source rate at current speed
				if p.st.Frames == 0 && isCopy(&s.Transcode) {
					if f := infoFPS(p.infoIn); f > 0 && p.st.Speed > 0 {
						p.st.FPS = math.Round(f*p.st.Speed*100) / 100
					}
				}
				// encoded output rate: each HTTP variant once, not every tee/HLS copy
				var w int64
				for _, h := range ohubs {
					w += h.Bytes()
				}
				if w == 0 {
					w = p.hub.Bytes()
				}
				if w == 0 {
					w = int64(sysmon.ProcWritten(cmd.Process.Pid))
				}
				if w > 0 {
					if !wroteAt.IsZero() && w >= wrote {
						if dt := time.Since(wroteAt).Seconds(); dt > 0 {
							p.st.Bitrate = float64(w-wrote) * 8 / 1000 / dt
						}
					}
					wrote, wroteAt = w, time.Now()
				}
				if !running {
					running = true
					p.mu.Unlock()
					m.setState(id, s.Name, p, Running, "")
					p.mu.Lock()
				}
			}
			p.mu.Unlock()
		}
	}()
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait(); close(errLines) }()

	timeout := time.Duration(max(st.StreamTimeout, 10)) * time.Second
	tk := time.NewTicker(2 * time.Second)
	defer tk.Stop()
	kill := func() {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(4 * time.Second):
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-exited
		}
	}
	tailStr := func() string {
		tmu.Lock()
		defer tmu.Unlock()
		for i := len(tail) - 1; i >= 0; i-- {
			if !isNoise(tail[i]) && strings.TrimSpace(tail[i]) != "" {
				return redact(tail[i])
			}
		}
		return ""
	}
	for {
		select {
		case <-ctx.Done():
			kill()
			return "stopped"
		case err := <-exited:
			p.mu.Lock()
			p.st.PID = 0
			p.mu.Unlock()
			msg := tailStr()
			if msg == "" && err != nil {
				msg = err.Error()
			}
			return "ffmpeg exited: " + msg
		case <-tk.C:
			p.mu.Lock()
			lim := timeout
			if p.st.Frames == 0 && !isCopy(&s.Transcode) {
				lim = max(timeout, 60*time.Second)
			}
			stalled := time.Since(p.last) > lim
			rss, ticks := sysmon.ProcStat(cmd.Process.Pid)
			if !p.tickT.IsZero() {
				dt := time.Since(p.tickT).Seconds()
				p.st.CPU = float64(ticks-p.ticks) / dt // percent of one core (100 ticks/s)
			}
			p.ticks, p.tickT, p.st.RSS = ticks, time.Now(), rss
			low := false
			if s.SpeedMin > 0 && p.st.State == Running && p.st.Speed > 0 && p.st.Speed < s.SpeedMin {
				if p.lowSince.IsZero() {
					p.lowSince = time.Now()
				}
				low = time.Since(p.lowSince) > 30*time.Second
			} else {
				p.lowSince = time.Time{}
			}
			memOver := s.MemMaxMB > 0 && rss > uint64(s.MemMaxMB)<<20
			p.mu.Unlock()
			if stalled {
				kill()
				if t := tailStr(); t != "" {
					return "stall: no frames for " + lim.String() + ": " + t
				}
				return "stall: no frames for " + lim.String()
			}
			if low {
				kill()
				return "speed below limit"
			}
			if memOver {
				kill()
				return "memory limit exceeded"
			}
			if s.Restart.Enabled && scheduled(s.Restart) {
				kill()
				return "scheduled restart"
			}
		}
	}
}

var lastSched sync.Map

func scheduled(r models.Restart) bool {
	now := time.Now()
	if len(r.Weekdays) > 0 && !containsInt(r.Weekdays, int(now.Weekday())) {
		return false
	}
	if !containsInt(r.Hours, now.Hour()) {
		return false
	}
	mins := r.Minutes
	if len(mins) == 0 {
		mins = []int{0}
	}
	if !containsInt(mins, now.Minute()) {
		return false
	}
	key := fmt.Sprintf("%p-%s", &r, now.Format("200601021504"))
	_, loaded := lastSched.LoadOrStore(key, true)
	return !loaded && now.Second() < 5
}

func containsInt(a []int, v int) bool {
	for _, x := range a {
		if x == v {
			return true
		}
	}
	return false
}

func isNoise(l string) bool {
	for _, n := range []string{"Opening '", "frame=", "Last message repeated", "deprecated pixel format", "Application provided invalid, non monotonically", "non-existing PPS"} {
		if strings.Contains(l, n) {
			return true
		}
	}
	return false
}

var credRe = regexp.MustCompile(`(?i)(://[^:/@\s]+:)[^@\s]+@|((?:pass(?:word|phrase)?|token|key)=)[^&\s]+`)

func redact(s string) string {
	return credRe.ReplaceAllString(s, "$1***@$2***")
}

func writeMaster(root string, vs []Variant) {
	byOut := map[string][]Variant{}
	for _, v := range vs {
		if v.Master {
			byOut[v.Output] = append(byOut[v.Output], v)
		}
	}
	for out, list := range byOut {
		var b strings.Builder
		b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
		for _, v := range list {
			bw := v.Bitrate * 1000
			if bw == 0 {
				bw = 3000000
			}
			res := ""
			if v.Size != "" && strings.Contains(v.Size, "x") {
				res = ",RESOLUTION=" + v.Size
			}
			fmt.Fprintf(&b, "#EXT-X-STREAM-INF:BANDWIDTH=%d%s,NAME=\"%s\"\n%s/index.m3u8\n", bw+128000, res, v.Name, v.Dir)
		}
		os.MkdirAll(root, 0o755)
		os.WriteFile(filepath.Join(root, out+".m3u8"), []byte(b.String()), 0o644)
	}
}

var hexAddr = regexp.MustCompile(`0x[0-9a-fA-F]+`)

// MergeProfile applies a profile the way Xaccel does: everything a profile
// defines (encoders, preset, overlay, audio, subtitle encoder) comes from the
// profile; per-stream maps, delogo and advanced MPEG-TS/color options stay.
func MergeProfile(p, s models.Transcode) models.Transcode {
	r := s
	r.HWAccel, r.VideoSync, r.VideoEncoders = p.HWAccel, p.VideoSync, p.VideoEncoders
	r.Preset, r.Profile, r.Level, r.Tune = p.Preset, p.Profile, p.Level, p.Tune
	r.FrameRate, r.GOP, r.BFrames, r.Deinterlace, r.PixFmt, r.DAR = p.FrameRate, p.GOP, p.BFrames, p.Deinterlace, p.PixFmt, p.DAR
	r.OverlayFile, r.OverlayX, r.OverlayY, r.OverlayOpacity, r.OverlayScale = p.OverlayFile, p.OverlayX, p.OverlayY, p.OverlayOpacity, p.OverlayScale
	r.Text, r.TextX, r.TextY, r.TextSize, r.TextColor, r.TextAlpha, r.TextScroll = p.Text, p.TextX, p.TextY, p.TextSize, p.TextColor, p.TextAlpha, p.TextScroll
	r.AudioEncoders, r.AudioChannels, r.AudioRate, r.AudioVolume = p.AudioEncoders, p.AudioChannels, p.AudioRate, p.AudioVolume
	r.AudioNormalize, r.AudioDelay = p.AudioNormalize, p.AudioDelay
	r.LowLatency = p.LowLatency || s.LowLatency
	r.SubMode = p.SubMode
	if p.SubFile != "" {
		r.SubFile = p.SubFile
	}
	if p.ExtraArgs != "" {
		r.ExtraArgs = p.ExtraArgs
	}
	return r
}

// viewer tracking for HLS (session based, counted by recent playlist hits)
var viewers sync.Map // id -> *sync.Map[key]time

type seen struct{ first, last time.Time }

func TouchViewer(id int64, key string) {
	v, _ := viewers.LoadOrStore(id, &sync.Map{})
	now := time.Now()
	if o, ok := v.(*sync.Map).Load(key); ok {
		v.(*sync.Map).Store(key, seen{o.(seen).first, now})
		return
	}
	v.(*sync.Map).Store(key, seen{now, now})
}

// hlsViewers lists HLS viewers seen in the last 30s.
func hlsViewers(id int64) []HubConn {
	var r []HubConn
	if v, ok := viewers.Load(id); ok {
		v.(*sync.Map).Range(func(k, t any) bool {
			if x := t.(seen); time.Since(x.last) < 30*time.Second {
				ip, _, _ := strings.Cut(k.(string), "|")
				r = append(r, HubConn{Addr: ip, Proto: "hls", Since: x.first})
			}
			return true
		})
	}
	return r
}

var (
	dumpHdr    = regexp.MustCompile(`^(Input|Output) #\d+`)
	dumpStream = regexp.MustCompile(`^\s*Stream #(\d+:\d+)(\[0x[0-9a-f]+\])?(\(\w+\))?.*?: (Video|Audio|Subtitle|Data): (.*)$`)
	dumpRes    = regexp.MustCompile(`\b\d{2,5}x\d{2,5}\b`)
	dumpNum    = regexp.MustCompile(`([\d.]+) (fps|Hz)`)
	dumpLayout = regexp.MustCompile(`\b(mono|stereo|[0-9]\.[0-9](\(\w+\))?|quad)\b`)
)

// dumpLine collects ffmpeg's "Stream #..." dump into a short Xaccel-style
// summary, e.g. "Stream #0:0[0x1f4]: Video: h264, yuv420p, 1920x1080, 25 fps".
func dumpLine(p *proc, l string) {
	if m := dumpHdr.FindStringSubmatch(l); m != nil {
		p.dumpOut = m[1] == "Output"
		if strings.HasPrefix(l, "Input #0") {
			p.infoIn, p.infoOut = nil, nil
		}
		return
	}
	m := dumpStream.FindStringSubmatch(l)
	if m == nil {
		return
	}
	rest, out := m[5], []string{strings.Fields(m[5])[0]}
	parts := strings.Split(rest, ", ")
	switch m[4] {
	case "Video":
		if len(parts) > 1 {
			out = append(out, strings.SplitN(parts[1], "(", 2)[0])
		}
		if r := dumpRes.FindString(rest); r != "" {
			out = append(out, r)
		}
		if n := dumpNum.FindStringSubmatch(rest); n != nil && n[2] == "fps" {
			out = append(out, n[1]+" fps")
		}
	case "Audio":
		if n := dumpNum.FindStringSubmatch(rest); n != nil && n[2] == "Hz" {
			out = append(out, n[1]+" Hz")
		}
		if c := dumpLayout.FindString(rest); c != "" {
			out = append(out, c)
		}
		if len(parts) > 3 {
			out = append(out, strings.Fields(parts[3])[0])
		}
	}
	line := "Stream #" + m[1] + m[2] + m[3] + ": " + m[4] + ": " + strings.Join(out, ", ")
	if p.dumpOut {
		p.infoOut = append(p.infoOut, line)
	} else {
		p.infoIn = append(p.infoIn, line)
	}
}

func infoFPS(lines []string) float64 {
	for _, l := range lines {
		if strings.Contains(l, ": Video: ") {
			if n := dumpNum.FindStringSubmatch(l); n != nil && n[2] == "fps" {
				f, _ := strconv.ParseFloat(n[1], 64)
				return f
			}
		}
	}
	return 0
}

// Details is the expanded stream view: stream dump and viewers.
type Details struct {
	In, Out []string
	Conns   []HubConn
	Outputs map[int]int // HTTP output number -> viewers
}

func (m *Manager) Details(id int64) Details {
	p := m.get(id)
	p.mu.Lock()
	d := Details{Outputs: map[int]int{}}
	d.In = append(d.In, p.infoIn...)
	d.Out = append(d.Out, p.infoOut...)
	hubs := map[int]*Hub{0: p.hub}
	for n, h := range p.ohubs {
		hubs[n] = h
	}
	p.mu.Unlock()
	for n, h := range hubs {
		c := h.Conns()
		d.Outputs[n] = h.Count()
		d.Conns = append(d.Conns, c...)
	}
	hls := hlsViewers(id)
	d.Outputs[-1] = len(hls)
	d.Conns = append(d.Conns, hls...)
	return d
}

func viewerCount(id int64) int {
	v, ok := viewers.Load(id)
	if !ok {
		return 0
	}
	n := 0
	v.(*sync.Map).Range(func(k, t any) bool {
		if time.Since(t.(seen).last) < 30*time.Second {
			n++
		} else {
			v.(*sync.Map).Delete(k)
		}
		return true
	})
	return n
}

// LastSegment returns the newest .ts file of a stream (used for thumbnails).
func LastSegment(root string) string {
	var best string
	var bt time.Time
	filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() && strings.HasSuffix(p, ".ts") && fi.ModTime().After(bt) && fi.Size() > 0 {
			best, bt = p, fi.ModTime()
		}
		return nil
	})
	return best
}

var _ = io.EOF

var demand sync.Map // id -> time of last viewer activity

func TouchDemand(id int64) { demand.Store(id, time.Now()) }

// StopIdle stops on-demand streams nobody watched for idle.
func (m *Manager) StopIdle(idle time.Duration) {
	for _, s := range m.All() {
		if !m.Running(s.ID) {
			continue
		}
		var x models.Stream
		if m.st.Get(models.KStream, s.ID, &x) != nil || !x.OnDemand || x.Enabled {
			continue
		}
		t, ok := demand.Load(s.ID)
		if (!ok || time.Since(t.(time.Time)) > idle) && s.Viewers == 0 {
			m.st.Log("stream", s.ID, "info", "on-demand idle, stopping")
			go m.Stop(s.ID)
		}
	}
}

// EnabledInputs drops sources disabled Xaccel-style with a numeric prefix
// before the scheme (e.g. "1http://...") or a leading "#"/"!".
func EnabledInputs(in []string) []string {
	out := make([]string, 0, len(in))
	for _, u := range in {
		u = strings.TrimSpace(u)
		if u == "" || u[0] == '#' || u[0] == '!' || (u[0] >= '0' && u[0] <= '9' && strings.Contains(u, "://")) {
			continue
		}
		out = append(out, u)
	}
	return out
}
