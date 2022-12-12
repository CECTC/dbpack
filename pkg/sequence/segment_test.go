package sequence

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"

	"github.com/cectc/dbpack/testdata"
)

type _SegmentTestSuite struct {
	suite.Suite
	mysql         testcontainers.Container
	segmentWorker *SegmentWorker
}

func TestSegment(t *testing.T) {
	suite.Run(t, new(_SegmentTestSuite))
}

func (suite *_SegmentTestSuite) SetupSuite() {
	mysql, err := testdata.NewSegmentEnvironment(suite.T(), "segment-test")
	if err != nil {
		suite.T().Fatal(err)
	}
	suite.mysql = mysql
	port, err := suite.mysql.MappedPort(context.Background(), "3306/tcp")
	if err != nil {
		suite.T().Fatal(err)
	}
	dsn := fmt.Sprintf("root:123456@tcp(localhost:%d)/segment", port.Int())
	segment, err := NewSegmentWorker(dsn, 0, 1000, "student")
	if err != nil {
		suite.T().Fatal(err)
	}
	suite.segmentWorker = segment
}

func (suite *_SegmentTestSuite) TestNextID() {
	nextID, err := suite.segmentWorker.NextID()
	assert.Nil(suite.T(), err)
	fmt.Println(nextID)
}

func (suite *_SegmentTestSuite) TearDownSuite() {
	minute := time.Minute
	err := suite.mysql.Stop(context.Background(), &minute)
	if err != nil {
		suite.T().Logf("Shutdown segment mysql failed, err: %v", err)
	}
}

func BenchmarkExample(b *testing.B) {
	s := new(_SegmentTestSuite)
	s.SetT(&testing.T{})
	s.SetupSuite()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		s.TestNextID()
		b.StopTimer()
	}
	s.TearDownSuite()
}
