//Problema 2: Produtor/Consumidor com Buffer Limitado

//Versão com semáforos: o buffer é uma estrutura de dados compartilhada, protegida
//explicitamente por semáforos de contagem (notEmpty, notFull) e por exclusão mútua nas
//posições de escrita e leitura, reproduzindo o algoritmo clássico visto em aula. Com múltiplos
//produtores e múltiplos consumidores, não basta contar itens: é preciso impedir que dois
//produtores escrevam na mesma posição e que dois consumidores retirem o mesmo item.

package main

import (
	"fmt"
	"sync"
	"time"
)

// Produtor/Consumidor com buffer limitado usando semáforos de contagem
// (notEmpty, notFull) e exclusão mútua nas posições de escrita e leitura.
// Usa apenas fmt, sync e time.

const (
	P = 4  // produtores
	C = 3  // consumidores
	K = 5  // capacidade do buffer
	N = 20 // itens produzidos por produtor
)

// ---------------------------------------------------------------------------
// Semáforo de contagem, implementado sobre um channel: cada token no channel
// é uma "unidade" do semáforo.
// ---------------------------------------------------------------------------

type Semaphore struct {
	tokens chan struct{}
}

func NewSemaphore(initial, max int) *Semaphore {
	s := &Semaphore{tokens: make(chan struct{}, max)}
	for i := 0; i < initial; i++ {
		s.tokens <- struct{}{}
	}
	return s
}

func (s *Semaphore) Wait()   { <-s.tokens }             // P(): bloqueia se valor == 0
func (s *Semaphore) Signal() { s.tokens <- struct{}{} } // V()

// TryWait é a versão não bloqueante de Wait.
func (s *Semaphore) TryWait() bool {
	select {
	case <-s.tokens:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Buffer circular compartilhado
//
//   notFull  (inicial K): conta posições livres
//   notEmpty (inicial 0): conta itens disponíveis
//   mutexP: exclusão mútua entre produtores (posição de escrita `in`)
//   mutexC: exclusão mútua entre consumidores (posição de leitura `out`)
//
// Os semáforos garantem que há espaço/item; os mutexes garantem que dois
// produtores não escrevam na mesma posição e que dois consumidores não
// retirem o mesmo item.
// ---------------------------------------------------------------------------

type Buffer struct {
	buf []int
	in  int // próxima posição de escrita (protegida por mutexP)
	out int // próxima posição de leitura (protegida por mutexC)

	notFull  *Semaphore
	notEmpty *Semaphore
	mutexP   sync.Mutex
	mutexC   sync.Mutex

	done chan struct{} // fechado quando todos os produtores terminaram
}

func NewBuffer(k int) *Buffer {
	return &Buffer{
		buf:      make([]int, k),
		notFull:  NewSemaphore(k, k),
		notEmpty: NewSemaphore(0, k),
		done:     make(chan struct{}),
	}
}

func (b *Buffer) Put(item int) {
	b.notFull.Wait() // espera haver posição livre

	b.mutexP.Lock()
	b.buf[b.in] = item
	b.in = (b.in + 1) % len(b.buf)
	b.mutexP.Unlock()

	b.notEmpty.Signal() // há mais um item disponível
}

// take retira o item da posição `out`. Só deve ser chamado depois de ter
// adquirido um token de notEmpty.
func (b *Buffer) take() int {
	b.mutexC.Lock()
	v := b.buf[b.out]
	b.out = (b.out + 1) % len(b.buf)
	b.mutexC.Unlock()

	b.notFull.Signal() // liberou uma posição
	return v
}

// Get bloqueia até haver item. Se timeout > 0, desiste após esse intervalo.
// Retorna (item, ok, timedOut). ok=false e timedOut=false significa
// "produtores terminaram e o buffer está vazio" (encerrar).
func (b *Buffer) Get(timeout time.Duration) (item int, ok bool, timedOut bool) {
	// Um channel nil bloqueia para sempre num select: sem timeout, esse case
	// nunca dispara.
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(timeout)
	}

	select {
	case <-b.notEmpty.tokens:
		return b.take(), true, false

	case <-b.done:
		// Produtores terminaram. Todo Signal de notEmpty acontece antes de o
		// produtor encerrar, então cada token restante corresponde a exatamente
		// um item no buffer. Drenamos até não sobrar token.
		if b.notEmpty.TryWait() {
			return b.take(), true, false
		}
		return 0, false, false

	case <-timeoutCh:
		return 0, false, true
	}
}

// ---------------------------------------------------------------------------

type Stats struct {
	mu       sync.Mutex
	produced int
	consumed int
	timeouts int
	seen     []int // seen[id] = quantas vezes o item id foi consumido (deve ser 1)
}

func (s *Stats) addProduced() {
	s.mu.Lock()
	s.produced++
	s.mu.Unlock()
}

func (s *Stats) addConsumed(item int) {
	s.mu.Lock()
	s.consumed++
	s.seen[item]++
	s.mu.Unlock()
}

func (s *Stats) addTimeout() int {
	s.mu.Lock()
	s.timeouts++
	n := s.timeouts
	s.mu.Unlock()
	return n
}

func producer(id int, buf *Buffer, st *Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < N; i++ {
		// "produz" (atraso determinístico, sem math/rand)
		time.Sleep(time.Duration(((id+1)*(i+3)*7)%40) * time.Millisecond)
		buf.Put(id*N + i) // bloqueia se cheio
		st.addProduced()
	}
}

func consumer(id int, buf *Buffer, st *Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	// O consumidor 0 usa timeout; os demais bloqueiam sem limite.
	var limite time.Duration
	if id == 0 {
		limite = 15 * time.Millisecond
	}

	for {
		item, ok, timedOut := buf.Get(limite)
		if timedOut {
			// Comportamento alternativo: nenhum item chegou a tempo.
			n := st.addTimeout()
			fmt.Printf("consumidor %d: timeout #%d (sem itens em %v)\n", id, n, limite)
			continue
		}
		if !ok { // produtores terminaram e buffer drenado
			return
		}
		st.addConsumed(item)
		time.Sleep(time.Duration((item%5)+1) * time.Millisecond) // "processa"
	}
}

func main() {
	total := P * N
	buf := NewBuffer(K)
	st := &Stats{seen: make([]int, total)}

	var prodWG, consWG sync.WaitGroup

	for c := 0; c < C; c++ {
		consWG.Add(1)
		go consumer(c, buf, st, &consWG)
	}
	for p := 0; p < P; p++ {
		prodWG.Add(1)
		go producer(p, buf, st, &prodWG)
	}

	// Encerramento ordenado
	prodWG.Wait()   // 1) todos os produtores terminam
	close(buf.done) // 2) avisa os consumidores
	consWG.Wait()   // 3) consumidores drenam o resto e encerram

	// Verificação
	bad := 0
	for _, n := range st.seen {
		if n != 1 {
			bad++
		}
	}
	fmt.Printf("\nesperado=%d produzido=%d consumido=%d timeouts=%d duplicados_ou_perdidos=%d\n",
		total, st.produced, st.consumed, st.timeouts, bad)

	if st.produced == st.consumed && st.produced == total && bad == 0 {
		fmt.Println("OK: total produzido == total consumido")
	} else {
		fmt.Println("FALHA: total produzido != total consumido")
	}
}
