package calculator

import (
	"elastic-proximity-calculation/src/elastic"
	"time"
)

type Config struct {
	Elastic              elastic.Config
	ProximityAmbit       int
	KeepAlive            int
	SourceIndex          string
	ProximityIndexPrefix string
	PageSize             int
	UploadChunkSize      int
	Start                time.Time
}
