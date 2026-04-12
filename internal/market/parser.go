package market

// Parser converts raw exchange messages into canonical structures
type Parser interface {
	ParseOrderBook(msg []byte) (*OrderBookUpdate, error)
	ParseOrderBookSnapshot(msg []byte, pair string) (*OrderBookUpdate, error)
}
