package sequence

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/proto"
)

func NewSequence(generator *config.SequenceGenerator, tableName string) (proto.SequenceGenerator, error) {
	switch generator.Type {
	case config.Segment:
		var (
			err           error
			content       []byte
			segmentConfig *SegmentConfig
		)
		if content, err = json.Marshal(generator.Config); err != nil {
			return nil, errors.Wrapf(err, "table %s marshal segment config failed.", tableName)
		}
		if err = json.Unmarshal(content, &segmentConfig); err != nil {
			log.Errorf("table %s unmarshal segment config failed, %v", tableName, err)
			return nil, err
		}
		if segmentConfig.Step == 0 {
			segmentConfig.Step = 1000
		}
		return NewSegmentWorker(segmentConfig.DSN, segmentConfig.Step, tableName)
	case config.Snowflake:
		var (
			err             error
			content         []byte
			snowflakeConfig *SnowflakeConfig
		)
		if content, err = json.Marshal(generator.Config); err != nil {
			return nil, errors.Wrapf(err, "table %s marshal snowflake config failed.", tableName)
		}
		if err = json.Unmarshal(content, &snowflakeConfig); err != nil {
			log.Errorf("table %s unmarshal snowflake config failed, %v", tableName, err)
			return nil, err
		}
		return NewWorker(snowflakeConfig.WorkerID)
	}
	return nil, errors.Errorf("table %s unsupported sequence %v", tableName, generator)
}

type SegmentConfig struct {
	DSN  string `yaml:"dsn" json:"dsn"`
	Step int64  `default:"1000" yaml:"step" json:"step"`
}

type SnowflakeConfig struct {
	WorkerID int64 `json:"worker_id"`
}
