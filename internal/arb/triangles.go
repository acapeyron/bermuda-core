package arb

import "strings"

// Triangle represents a 3-leg arbitrage cycle.
type Triangle struct {
	Name string
	Legs [3]Leg
}

// knownQuotes is the list of known quote currencies, ordered by priority.
// Used to split a symbol into base+quote.
var knownQuotes = []string{"USDT", "USDC", "BTC", "ETH", "BNB"}

type edge struct {
	pair string
	from string
	to   string
	side string // "buy" (spend quote, get base) or "sell" (spend base, get quote)
}

func splitPair(symbol string) (base, quote string, ok bool) {
	for _, q := range knownQuotes {
		if strings.HasSuffix(symbol, q) {
			return strings.TrimSuffix(symbol, q), q, true
		}
	}
	return "", "", false
}

// buildEdges returns all directed edges from a list of pair symbols.
// Each pair produces two edges (both directions).
func buildEdges(pairs []string) []edge {
	var edges []edge
	for _, pair := range pairs {
		base, quote, ok := splitPair(pair)
		if !ok {
			continue
		}
		// buy: spend quote currency, receive base currency
		edges = append(edges, edge{pair: pair, from: quote, to: base, side: "buy"})
		// sell: spend base currency, receive quote currency
		edges = append(edges, edge{pair: pair, from: base, to: quote, side: "sell"})
	}
	return edges
}

// GenerateTriangles finds all unique 3-leg cycles from the given pair symbols.
func GenerateTriangles(pairs []string) []Triangle {
	edges := buildEdges(pairs)

	// Index edges by their "from" currency for fast lookup
	byFrom := make(map[string][]edge)
	for _, e := range edges {
		byFrom[e.from] = append(byFrom[e.from], e)
	}

	var triangles []Triangle
	seen := make(map[string]bool)

	for _, e1 := range edges {
		for _, e2 := range byFrom[e1.to] {
			if e2.pair == e1.pair {
				continue
			}
			for _, e3 := range byFrom[e2.to] {
				if e3.pair == e1.pair || e3.pair == e2.pair {
					continue
				}
				// Cycle closes if e3 leads back to e1.from
				if e3.to != e1.from {
					continue
				}

				// Deduplicate: canonical key = sorted pair names
				key := canonicalKey(e1.pair, e2.pair, e3.pair)
				if seen[key] {
					continue
				}
				seen[key] = true

				name := e1.from + "→" + e1.to + "→" + e2.to + "→" + e3.to
				triangles = append(triangles, Triangle{
					Name: name,
					Legs: [3]Leg{
						{Pair: e1.pair, Side: e1.side},
						{Pair: e2.pair, Side: e2.side},
						{Pair: e3.pair, Side: e3.side},
					},
				})
			}
		}
	}

	return triangles
}

func canonicalKey(a, b, c string) string {
	s := []string{a, b, c}
	// simple sort
	if s[0] > s[1] {
		s[0], s[1] = s[1], s[0]
	}
	if s[1] > s[2] {
		s[1], s[2] = s[2], s[1]
	}
	if s[0] > s[1] {
		s[0], s[1] = s[1], s[0]
	}
	return s[0] + "|" + s[1] + "|" + s[2]
}
