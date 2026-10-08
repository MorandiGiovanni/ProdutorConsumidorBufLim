//Problema 2: Produtor/Consumidor com Buffer Limitado

//Versão com channel: o buffer é um channel com capacidade K, usando a semântica de
//bloqueio do próprio Go

package main

import (
	"fmt"
	"sync"
	"time"
)

// Produtor/Consumidor com buffer limitado: o buffer é um channel com
// capacidade K. O bloqueio (cheio/vazio) é a semântica própria do Go.
// Usa apenas fmt, sync e time.

const (
	P = 4  // produtores
	C = 3  // consumidores
	K = 5  // capacidade do buffer
	N = 20 // itens produzidos por produtor
)

// Estatísticas compartilhadas, protegidas por mutex.
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

func producer(id int, buffer chan<- int, st *Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < N; i++ {
		// "produz" (atraso determinístico, sem math/rand)
		time.Sleep(time.Duration(((id+1)*(i+3)*7)%40) * time.Millisecond)
		buffer <- id*N + i // bloqueia se o buffer estiver cheio
		st.addProduced()
	}
}

func consumer(id int, buffer <-chan int, st *Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	process := func(item int) {
		st.addConsumed(item)
		time.Sleep(time.Duration((item%5)+1) * time.Millisecond) // "processa"
	}

	if id == 0 {
		// Consumidor com timeout: select + time.After.
		const limite = 15 * time.Millisecond
		for {
			select {
			case item, ok := <-buffer:
				if !ok { // channel fechado e já drenado
					return
				}
				process(item)
			case <-time.After(limite):
				// Comportamento alternativo: nenhum item chegou a tempo.
				n := st.addTimeout()
				fmt.Printf("consumidor %d: timeout #%d (sem itens em %v)\n", id, n, limite)
			}
		}
	}

	// Demais consumidores: bloqueiam até haver item. O range termina quando o
	// channel é fechado E esvaziado, então drena todos os itens restantes.
	for item := range buffer {
		process(item)
	}
}

func main() {
	total := P * N
	buffer := make(chan int, K)
	st := &Stats{seen: make([]int, total)}

	var prodWG, consWG sync.WaitGroup

	for c := 0; c < C; c++ {
		consWG.Add(1)
		go consumer(c, buffer, st, &consWG)
	}
	for p := 0; p < P; p++ {
		prodWG.Add(1)
		go producer(p, buffer, st, &prodWG)
	}

	// Encerramento ordenado
	prodWG.Wait() // 1) todos os produtores terminam
	close(buffer) // 2) fecha o channel (itens no buffer continuam disponíveis)
	consWG.Wait() // 3) consumidores drenam o resto e encerram

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
