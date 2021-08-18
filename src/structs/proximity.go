package structs

import "sync"

type Proximity map[string]interface{}

func CreateProximityObject(sourceIndex string, sourceId string, sourceField string, num float64) Proximity {
	return Proximity{
		"source_index": sourceIndex,
		"source_id":    sourceId,
		"source_field": sourceField,
		"num":          num,
	}
}

type Container struct {
	mx sync.RWMutex
	m  map[string][]*Proximity
}

func (c *Container) Add(language string, proximity *Proximity) {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.m[language] = append(c.m[language], proximity)
}

func (c *Container) CheckTotalLength(uploadChunkSize int) bool {
	c.mx.RLock()
	defer c.mx.RUnlock()

	var total int = 0
	for _, proximities := range c.m {
		total += len(proximities)
	}

	return total >= uploadChunkSize
}

func (c *Container) GetByLanguage(language string) []*Proximity {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.m[language]
}

func (c *Container) GetAll() map[string][]*Proximity {
	c.mx.RLock()
	defer c.mx.RUnlock()

	return c.m
}

func (c *Container) DeleteByLanguage(language string) {
	c.mx.Lock()
	defer c.mx.Unlock()

	delete(c.m, language)
}

func NewContainer() *Container {
	return &Container{
		m: make(map[string][]*Proximity),
	}
}
