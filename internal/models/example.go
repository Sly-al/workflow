package models

type Example struct {
	ID      int    `json:"ID"`
	Message string `json:"Message"`
}

type ExampleResult struct {
	Duration string `json:"duration"`
	ID       int    `json:"ID"`
}
