//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// as of Go 1.8 this requires the goyacc external tool
// available from golang.org/x/tools/cmd/goyacc

//go:generate goyacc -o query_string.y.go query_string.y
//go:generate sed -i.tmp -e 1d query_string.y.go
//go:generate rm query_string.y.go.tmp
//go:generate gofmt -s -w query_string.y.go

// note: OSX sed and gnu sed handle the -i (in-place) option differently.
// using -i.tmp works on both, at the expense of having to remove
// the unsightly .tmp files

package querystr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/blugelabs/bluge"
)

type QueryStringOptions struct {
	debugParser bool
	debugLexer  bool
	dateFormat  string
	logger      *log.Logger
}

func DefaultOptions() QueryStringOptions {
	return QueryStringOptions{
		dateFormat: time.RFC3339,
	}
}

func (o QueryStringOptions) WithDebugParser(debug bool) QueryStringOptions {
	o.debugParser = debug
	return o
}

func (o QueryStringOptions) WithDebugLexer(debug bool) QueryStringOptions {
	o.debugLexer = debug
	return o
}

func (o QueryStringOptions) WithDateFormat(dateFormat string) QueryStringOptions {
	o.dateFormat = dateFormat
	return o
}

func (o QueryStringOptions) WithLogger(logger *log.Logger) QueryStringOptions {
	o.logger = logger
	return o
}

func ParseQueryString(query string, options QueryStringOptions) (rq bluge.Query, err error) {
	if query == "" {
		return bluge.NewMatchNoneQuery(), nil
	}
	lex := newLexerWrapper(newQueryStringLex(strings.NewReader(query), options), options)
	doParse(lex)

	if len(lex.errs) > 0 {
		return nil, fmt.Errorf(strings.Join(lex.errs, "\n"))
	}
	return lex.query, nil
}

func doParse(lex *lexerWrapper) {
	defer func() {
		r := recover()
		if r != nil {
			lex.errs = append(lex.errs, fmt.Sprintf("parse error: %v", r))
		}
	}()

	yyParse(lex)
}

const (
	queryShould = iota
	queryMust
	queryMustNot
)

type lexerWrapper struct {
	lex         yyLexer
	errs        []string
	query       *bluge.BooleanQuery
	debugParser bool
	dateFormat  string
	logger      *log.Logger
}

func newLexerWrapper(lex yyLexer, options QueryStringOptions) *lexerWrapper {
	return &lexerWrapper{
		lex:         lex,
		query:       bluge.NewBooleanQuery(),
		debugParser: options.debugParser,
		dateFormat:  options.dateFormat,
		logger:      options.logger,
	}
}

func (l *lexerWrapper) Lex(lval *yySymType) int {
	return l.lex.Lex(lval)
}

func (l *lexerWrapper) Error(s string) {
	l.errs = append(l.errs, s)
}

func (l *lexerWrapper) logDebugGrammarf(format string, v ...interface{}) {
	if l.debugParser {
		l.logger.Printf(format, v...)
	}
}

func queryTimeFromString(yylex yyLexer, t string) (time.Time, error) {
	rv, err := time.Parse(yylex.(*lexerWrapper).dateFormat, t)
	if err != nil {
		return time.Time{}, err
	}
	return rv, nil
}

func queryStringStringToken(field, str string) bluge.Query {
	if strings.HasPrefix(str, "/") && strings.HasSuffix(str, "/") {
		return bluge.NewRegexpQuery(str[1 : len(str)-1]).SetField(field)
	} else if strings.ContainsAny(str, "*?") {
		return bluge.NewWildcardQuery(str).SetField(field)
	}
	return bluge.NewMatchQuery(str).SetField(field)
}

func queryStringStringTokenFuzzy(field, str, fuzziness string) (*bluge.MatchQuery, error) {
	fuzzy, err := strconv.ParseFloat(fuzziness, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid fuzziness value: %v", err)
	}
	return bluge.NewMatchQuery(str).SetFuzziness(int(fuzzy)).SetField(field), nil
}

func queryStringNumberToken(field, str string) (bluge.Query, error) {
	q1 := bluge.NewMatchQuery(str).SetField(field)
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing number: %v", err)
	}
	q2 := bluge.NewNumericRangeInclusiveQuery(val, val, true, true).SetField(field)
	return bluge.NewBooleanQuery().AddShould([]bluge.Query{q1, q2}...), nil
}

func queryStringPhraseToken(field, str string) *bluge.MatchPhraseQuery {
	return bluge.NewMatchPhraseQuery(str).SetField(field)
}

func queryStringNumericRangeGreaterThanOrEqual(field, str string, orEqual bool) (*bluge.NumericRangeQuery, error) {
	min, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing number: %v", err)
	}
	return bluge.NewNumericRangeInclusiveQuery(min, bluge.MaxNumeric, orEqual, true).
		SetField(field), nil
}

func queryStringNumericRangeLessThanOrEqual(field, str string, orEqual bool) (*bluge.NumericRangeQuery, error) {
	max, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing number: %v", err)
	}
	return bluge.NewNumericRangeInclusiveQuery(bluge.MinNumeric, max, true, orEqual).
		SetField(field), nil
}

func queryStringDateRangeGreaterThanOrEqual(yylex yyLexer, field, phrase string, orEqual bool) (*bluge.DateRangeQuery, error) {
	minTime, err := queryTimeFromString(yylex, phrase)
	if err != nil {
		return nil, fmt.Errorf("invalid time: %v", err)
	}
	return bluge.NewDateRangeInclusiveQuery(minTime, time.Time{}, orEqual, true).
		SetField(field), nil
}

func queryStringDateRangeLessThanOrEqual(yylex yyLexer, field, phrase string, orEqual bool) (*bluge.DateRangeQuery, error) {
	maxTime, err := queryTimeFromString(yylex, phrase)
	if err != nil {
		return nil, fmt.Errorf("invalid time: %v", err)
	}
	return bluge.NewDateRangeInclusiveQuery(time.Time{}, maxTime, true, orEqual).
		SetField(field), nil
}

const noBoost = 1.0

func queryStringParseBoost(str string) (float64, error) {
	boost, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return noBoost, fmt.Errorf("invalid boost value: %v", err)
	}
	return boost, nil
}

func queryStringSetBoost(q bluge.Query, b float64) (bluge.Query, error) {
	switch v := q.(type) {
	case *bluge.MatchQuery:
		return v.SetBoost(b), nil
	case *bluge.RegexpQuery:
		return v.SetBoost(b), nil
	case *bluge.WildcardQuery:
		return v.SetBoost(b), nil
	case *bluge.BooleanQuery:
		return v.SetBoost(b), nil
	case *bluge.NumericRangeQuery:
		return v.SetBoost(b), nil
	case *bluge.MatchPhraseQuery:
		return v.SetBoost(b), nil
	case *bluge.DateRangeQuery:
		return v.SetBoost(b), nil
	}
	return nil, fmt.Errorf("cannot boost %T", q)
}
