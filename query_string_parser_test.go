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

package querystr

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
)

func TestQuerySyntaxParserValid(t *testing.T) {
	theDate, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		input  string
		result bluge.Query
	}{
		{
			input: "test",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("test")),
		},
		{
			input: "127.0.0.1",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("127.0.0.1")),
		},
		{
			input: `"test phrase 1"`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchPhraseQuery("test phrase 1")),
		},
		{
			input: "field:test",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("test").SetField("field")),
		},
		// - is allowed inside a term, just not the start
		{
			input: "field:t-est",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("t-est").SetField("field")),
		},
		// + is allowed inside a term, just not the start
		{
			input: "field:t+est",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("t+est").SetField("field")),
		},
		// > is allowed inside a term, just not the start
		{
			input: "field:t>est",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("t>est").SetField("field")),
		},
		// < is allowed inside a term, just not the start
		{
			input: "field:t<est",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("t<est").SetField("field")),
		},
		// = is allowed inside a term, just not the start
		{
			input: "field:t=est",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("t=est").SetField("field")),
		},
		{
			input: "+field1:test1",
			result: bluge.NewBooleanQuery().
				AddMust(bluge.NewMatchQuery("test1").SetField("field1")),
		},
		{
			input: "-field2:test2",
			result: bluge.NewBooleanQuery().
				AddMustNot(bluge.NewMatchQuery("test2").SetField("field2")),
		},
		{
			input: `field3:"test phrase 2"`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchPhraseQuery("test phrase 2").SetField("field3")),
		},
		{
			input: `+field4:"test phrase 1"`,
			result: bluge.NewBooleanQuery().
				AddMust(bluge.NewMatchPhraseQuery("test phrase 1").SetField("field4")),
		},
		{
			input: `-field5:"test phrase 2"`,
			result: bluge.NewBooleanQuery().
				AddMustNot(bluge.NewMatchPhraseQuery("test phrase 2").SetField("field5")),
		},
		{
			input: `+field6:test3 -field7:test4 field8:test5`,
			result: bluge.NewBooleanQuery().
				AddMust(bluge.NewMatchQuery("test3").SetField("field6")).
				AddShould(bluge.NewMatchQuery("test5").SetField("field8")).
				AddMustNot(bluge.NewMatchQuery("test4").SetField("field7")),
		},
		{
			input: "test^3",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("test").SetBoost(3.0)),
		},
		{
			input: "test^3 other^6",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("test").SetBoost(3.0)).
				AddShould(bluge.NewMatchQuery("other").SetBoost(6.0)),
		},
		{
			input: "33",
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewBooleanQuery().
						AddShould(bluge.NewMatchQuery("33")).
						AddShould(
							bluge.NewNumericRangeInclusiveQuery(33.0, 33.0,
								true, true))),
		},
		{
			input: "field:33",
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewBooleanQuery().
						AddShould(bluge.NewMatchQuery("33").SetField("field")).
						AddShould(
							bluge.NewNumericRangeInclusiveQuery(33.0, 33.0,
								true, true).
								SetField("field"))),
		},
		{
			input: "cat-dog",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("cat-dog")),
		},
		{
			input: "watex~",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("watex").SetFuzziness(1)),
		},
		{
			input: "watex~2",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("watex").SetFuzziness(2)),
		},
		{
			input: "watex~ 2",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("watex").SetFuzziness(1)).
				AddShould(bluge.NewBooleanQuery().
					AddShould(bluge.NewMatchQuery("2")).
					AddShould(
						bluge.NewNumericRangeInclusiveQuery(2.0, 2.0, true, true))),
		},
		{
			input: "field:watex~",
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewMatchQuery("watex").
						SetFuzziness(1).
						SetField("field")),
		},
		{
			input: "field:watex~2",
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("watex").SetFuzziness(2).SetField("field")),
		},
		{
			input: `field:555c3bb06f7a127cda000005`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("555c3bb06f7a127cda000005").SetField("field")),
		},
		{
			input: `field:>5`,
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewNumericRangeInclusiveQuery(5.0, bluge.MaxNumeric, false, true).
						SetField("field")),
		},
		{
			input: `field:>=5`,
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewNumericRangeInclusiveQuery(5.0, bluge.MaxNumeric, true, true).
						SetField("field")),
		},
		{
			input: `field:<5`,
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewNumericRangeInclusiveQuery(bluge.MinNumeric, 5.0, true, false).
						SetField("field")),
		},
		{
			input: `field:<=5`,
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewNumericRangeInclusiveQuery(bluge.MinNumeric, 5.0, true, true).
						SetField("field")),
		},
		// new range tests with negative number
		{
			input: "field:-5",
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewBooleanQuery().
						AddShould(
							bluge.NewMatchQuery("-5").SetField("field")).
						AddShould(
							bluge.NewNumericRangeInclusiveQuery(-5.0, -5.0, true, true).
								SetField("field"))),
		},
		{
			input: `field:>-5`,
			result: bluge.NewBooleanQuery().
				AddShould(
					bluge.NewNumericRangeInclusiveQuery(-5.0, bluge.MaxNumeric, false, true).
						SetField("field")),
		},
		{
			input: `field:>=-5`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewNumericRangeInclusiveQuery(-5.0, bluge.MaxNumeric, true, true).
					SetField("field")),
		},
		{
			input: `field:<-5`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewNumericRangeInclusiveQuery(bluge.MinNumeric, -5.0, true, false).
					SetField("field")),
		},
		{
			input: `field:<=-5`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewNumericRangeInclusiveQuery(bluge.MinNumeric, -5.0, true, true).
					SetField("field")),
		},
		{
			input: `field:>"2006-01-02T15:04:05Z"`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewDateRangeInclusiveQuery(theDate, time.Time{}, false, true).
					SetField("field")),
		},
		{
			input: `field:>="2006-01-02T15:04:05Z"`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewDateRangeInclusiveQuery(theDate, time.Time{}, true, true).
					SetField("field")),
		},
		{
			input: `field:<"2006-01-02T15:04:05Z"`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewDateRangeInclusiveQuery(time.Time{}, theDate, true, false).
					SetField("field")),
		},
		{
			input: `field:<="2006-01-02T15:04:05Z"`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewDateRangeInclusiveQuery(time.Time{}, theDate, true, true).
					SetField("field")),
		},
		{
			input: `/mar.*ty/`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewRegexpQuery("mar.*ty")),
		},
		{
			input: `name:/mar.*ty/`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewRegexpQuery("mar.*ty").
					SetField("name")),
		},
		{
			input: `mart*`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewWildcardQuery("mart*")),
		},
		{
			input: `name:mart*`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewWildcardQuery("mart*").
					SetField("name")),
		},

		// tests for escaping

		// escape : as field delimeter
		{
			input: `name\:marty`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("name:marty")),
		},
		// first colon delimiter, second escaped
		{
			input: `name:marty\:couchbase`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("marty:couchbase").
					SetField("name")),
		},
		// escape space, single arguemnt to match query
		{
			input: `marty\ couchbase`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("marty couchbase")),
		},
		// escape leading plus, not a must clause
		{
			input: `\+marty`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("+marty")),
		},
		// escape leading minus, not a must not clause
		{
			input: `\-marty`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery("-marty")),
		},
		// escape quote inside of phrase
		{
			input: `"what does \"quote\" mean"`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchPhraseQuery(`what does "quote" mean`)),
		},
		// escaping an unsupported character retains backslash
		{
			input: `can\ i\ escap\e`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery(`can i escap\e`)),
		},
		// leading spaces
		{
			input: `   what`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery(`what`)),
		},
		// no boost value defaults to 1
		{
			input: `term^`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery(`term`).
					SetBoost(1.0)),
		},
		// weird lexer cases, something that starts like a number
		// but contains escape and ends up as string
		{
			input: `3.0\:`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery(`3.0:`)),
		},
		{
			input: `3.0\a`,
			result: bluge.NewBooleanQuery().
				AddShould(bluge.NewMatchQuery(`3.0\a`)),
		},
	}

	for _, test := range tests {

		q, err := ParseQueryString(test.input, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(q, test.result) {
			t.Errorf("Expected %#v, got %#v: for %s", test.result, q, test.input)
		}
	}
}

func TestQuerySyntaxParserInvalid(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"^"},
		{"^5"},
		{"field:-text"},
		{"field:+text"},
		{"field:>text"},
		{"field:>=text"},
		{"field:<text"},
		{"field:<=text"},
		{"field:~text"},
		{"field:^text"},
		{"field::text"},
		{`"this is the time`},
		{`cat^3\:`},
		{`cat^3\0`},
		{`cat~3\:`},
		{`cat~3\0`},
		{strings.Repeat(`9`, 369)},
		{`field:` + strings.Repeat(`9`, 369)},
		{`field:>` + strings.Repeat(`9`, 369)},
		{`field:>=` + strings.Repeat(`9`, 369)},
		{`field:<` + strings.Repeat(`9`, 369)},
		{`field:<=` + strings.Repeat(`9`, 369)},
	}

	for _, test := range tests {
		_, err := ParseQueryString(test.input, DefaultOptions())
		if err == nil {
			t.Errorf("expected error, got nil for `%s`", test.input)
		}
	}
}

var extTokenTypes []int
var extTokens []yySymType

func BenchmarkLexer(b *testing.B) {

	for n := 0; n < b.N; n++ {
		var tokenTypes []int
		var tokens []yySymType
		r := strings.NewReader(`+field4:"test phrase 1"`)
		l := newQueryStringLex(r, DefaultOptions())
		var lval yySymType
		rv := l.Lex(&lval)
		for rv > 0 {
			tokenTypes = append(tokenTypes, rv)
			tokens = append(tokens, lval)
			lval.s = ""
			lval.n = 0
			rv = l.Lex(&lval)
		}
		extTokenTypes = tokenTypes
		extTokens = tokens
	}

}
