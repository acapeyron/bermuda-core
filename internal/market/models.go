package market

type Level struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

type OrderBookUpdate struct {
	Pair         string  `json:"pair"`
	Bids         []Level `json:"bids"`
	Asks         []Level `json:"asks"`
	Timestamp    int64   `json:"timestamp"`
	LastUpdateID int64   `json:"last_update_id"`
}
