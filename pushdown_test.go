package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func randStr(length int) string {
	bs := make([]rune, length)
	for i := 0; i < length; i++ {
		r := rand.Intn(50)
		if r >= 26 {
			r += 7
		}
		bs[i] = 'A' + rune(r)
	}
	return string(bs)
}

func TestIndexLookupPushDown(t *testing.T) {
	var (
		host     string
		port     int
		database string

		twoColPk bool
		id1Type  string
		id2Type  string
	)

	host = "127.0.0.1"
	port = 4001
	database = "test"

	twoColPk = true
	id1Type = "bigint not null"
	id2Type = "varchar(64) COLLATE utf8mb4_general_ci not null"

	ctx := context.TODO()
	pkList := "id"
	if twoColPk {
		pkList = "id, id2"
	}

	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%d)/%s", host, port, database))
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.Exec("drop table if exists test_index_lookup_push_down")
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`create table test_index_lookup_push_down (
id %s,
id2 %s,
ss varchar(64) default 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' not null,
k bigint,
uk bigint,
v1 bigint,
v2 varchar(255) not null,
PRIMARY KEY (%s) /*T![clustered_index] CLUSTERED */,
key idx_k (k),
unique key idx_uk (uk)
)`, id1Type, id2Type, pkList))
	require.NoError(t, err)
	kValCnt := 1024
	pkPerKVal := 256
	totalInsert := kValCnt * pkPerKVal
	kMap := make(map[int64]map[string]struct{})
	id2KMap := make(map[string]int64)
	for i := 0; i < totalInsert; {
		var sql strings.Builder
		sql.WriteString("insert into test_index_lookup_push_down (id, id2, k, uk, v1, v2) values")
		batchCnt := 256
		args := make([]any, 0, 4*batchCnt)
		for batch := 0; batch < batchCnt && i < totalInsert; batch, i = batch+1, i+1 {
			id := int64(i + 1)
			id2 := randStr(16)
			idStr := fmt.Sprintf("%d+%s", id, id2)
			k := rand.Int63n(int64(kValCnt)) + 1
			uk := rand.Int63()
			v1 := rand.Int63()
			v2 := uuid.NewString()
			if batch != 0 {
				sql.WriteRune(',')
			}
			sql.WriteString(" (?, ?, ?, ?, ?, ?)")
			args = append(args, id, id2, k, uk, v1, v2)
			m, ok := kMap[k]
			if !ok {
				m = make(map[string]struct{})
				kMap[k] = m
			}
			m[idStr] = struct{}{}
			id2KMap[idStr] = k
		}
		_, err = db.Exec(sql.String(), args...)
		require.NoError(t, err)
		log.Printf("write %d rows, progress: %.0f%%\n", i, float64(i)/float64(totalInsert)*100.0)
	}

	type Case struct {
		k         int64
		kRange    []int64
		kIn       []int64
		uniqueIdx bool
		skip      int
		limit     int
		sort      bool
		desc      bool
	}

	cases := make([]Case, 0)
	for i := 0; i < 10; i++ {
		rangeStart := rand.Int63n(int64(kValCnt)) + 1
		kIn := make([]int64, rand.Intn(8)+1)
		for j := 0; j < len(kIn); j++ {
			if j == 0 {
				kIn[j] = rand.Int63n(int64(kValCnt))
			} else {
				kIn[j] = kIn[j-1] + rand.Int63n(4) + 1
			}
		}
		cases = append(
			cases,
			Case{
				k: rand.Int63n(int64(kValCnt)+10) + 1,
			},
			Case{
				k:     rand.Int63n(int64(kValCnt)+10) + 1,
				limit: rand.Intn(128) + 1,
			},
			Case{
				kRange: []int64{
					rangeStart,
					rangeStart + rand.Int63n(4),
				},
			},
			Case{
				kRange: []int64{
					rangeStart,
					rangeStart + rand.Int63n(4),
				},
				limit: rand.Intn(128),
			},
			Case{
				kIn: kIn,
			},
			Case{
				kIn:   kIn,
				limit: rand.Intn(128),
			},
			Case{
				kIn:  kIn,
				sort: true,
			},
			Case{
				kIn:  kIn,
				sort: true,
				desc: true,
			},
			Case{
				kRange: []int64{
					rangeStart,
					rangeStart + rand.Int63n(4),
				},
				limit: rand.Intn(128) + 1,
			},
		)
	}

	runCases := func() {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, conn.Close())
		}()
		_, err = conn.ExecContext(ctx, "set @@session.tidb_session_alias = 'test'")
		require.NoError(t, err)
		for _, c := range cases {
			require.True(t, c.k > 0 || len(c.kIn) > 0 || len(c.kRange) > 0)
			require.False(t, len(c.kIn) > 0 && len(c.kRange) > 0)
			var involvedIndices []int64
			involvedIDs := make([]string, 0, 1024)
			if c.k > 0 {
				require.Empty(t, c.kRange)
				require.Empty(t, c.kIn)
				involvedIndices = []int64{c.k}
			} else if len(c.kRange) > 0 {
				require.Zero(t, c.kIn)
				require.Empty(t, c.kIn)
				involvedIndices = make([]int64, 0, c.kRange[1]-c.kRange[0])
				for i := c.kRange[0]; i < c.kRange[1]; i++ {
					involvedIndices = append(involvedIndices, i)
				}
			} else {
				require.NotEmpty(t, c.kIn)
				require.Zero(t, c.k)
				require.Empty(t, c.kRange)
				involvedIndices = append(involvedIndices, c.kIn...)
			}

			for _, k := range involvedIndices {
				if pks, ok := kMap[k]; ok {
					for pk := range pks {
						involvedIDs = append(involvedIDs, pk)
					}
				}
			}

			var sb strings.Builder
			sb.WriteString("select /*+ use_index(test_index_lookup_push_down, idx_k) */ id,id2,k,uk,v1,v2 from test_index_lookup_push_down")
			switch {
			case c.k > 0:
				sb.WriteString(fmt.Sprintf(" where k = %d", c.k))
			case len(c.kRange) > 0:
				sb.WriteString(fmt.Sprintf(" where k >= %d AND k < %d", c.kRange[0], c.kRange[1]))
			case len(c.kIn) > 0:
				ks := make([]string, 0, len(c.kIn))
				for _, k := range c.kIn {
					ks = append(ks, strconv.FormatInt(k, 10))
				}
				sb.WriteString(fmt.Sprintf(" where k in (%s)", strings.Join(ks, ",")))
			}

			if c.sort {
				sb.WriteString(" order by k")
			}

			if c.desc {
				sb.WriteString(" desc")
			}

			if c.limit > 0 {
				if c.skip > 0 {
					sb.WriteString(fmt.Sprintf(" limit %d, %d", c.skip, c.limit))
				} else {
					sb.WriteString(fmt.Sprintf(" limit %d", c.limit))
				}
			}

			sqlText := sb.String()
			log.Printf("Case: \n\tSQL: %s\n", sqlText)

			rows, err := conn.QueryContext(ctx, sqlText)
			require.NoError(t, err)
			gotIDs := make([]string, 0)
			for rows.Next() {
				var id, k, uk, v1 int64
				var id2, v2 string
				err = rows.Scan(&id, &id2, &k, &uk, &v1, &v2)
				require.NoError(t, err)
				idStr := fmt.Sprintf("%d+%s", id, id2)
				require.NotContains(t, gotIDs, idStr)
				gotIDs = append(gotIDs, idStr)
			}

			require.Subset(t, involvedIDs, gotIDs)
			expectedCnt := len(involvedIDs)
			if c.skip > 0 {
				expectedCnt -= c.skip
			}

			if c.limit > 0 && c.limit < expectedCnt {
				expectedCnt = c.limit
			}

			if expectedCnt < 0 {
				expectedCnt = 0
			}

			require.Equal(t, expectedCnt, len(gotIDs))
			if c.sort {
				var prev int64
				for i, idStr := range gotIDs {
					k := id2KMap[idStr]
					if i > 0 {
						if c.desc {
							require.LessOrEqual(t, k, prev, "%d, %s", i, idStr)
						} else {
							require.GreaterOrEqual(t, k, prev, "%d, %s", i, idStr)
						}
					}
					prev = k
				}
			}
		}
	}

	var splitQuery string
	if twoColPk {
		splitQuery = fmt.Sprintf("split table test_index_lookup_push_down between (0, 'a') and (%d, 'Z') regions 10\n", kValCnt*pkPerKVal)
	} else {
		splitQuery = fmt.Sprintf("split table test_index_lookup_push_down between (0) and (%d) regions 10\n", kValCnt*pkPerKVal)
	}
	_, err = db.Exec(splitQuery)
	require.NoError(t, err)
	log.Printf("sleep 2 seconds...")
	time.Sleep(2 * time.Second)
	runCases()

	if twoColPk {
		splitQuery = fmt.Sprintf("split table test_index_lookup_push_down between (0, 'A') and (%d, 'z') regions 200\n", kValCnt*pkPerKVal)
	} else {
		splitQuery = fmt.Sprintf("split table test_index_lookup_push_down between (0) and (%d) regions 200\n", kValCnt*pkPerKVal)
	}
	_, err = db.Exec(splitQuery)
	require.NoError(t, err)
	log.Printf("sleep 2 seconds...")
	time.Sleep(2 * time.Second)
	runCases()
}
