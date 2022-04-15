package parquet

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/segmentio/parquet-go/format"
)

func ExampleColumnLayout_String() {
	fmt.Println(ColumnLayout{
		{"id"},
		{"details", "first_name"},
		{"details", "last_name"},
	})

	// Output:
	// id,details.first_name,details.last_name
}

func TestColumnLayoutOrderedSchemaElements(t *testing.T) {
	tests := []struct {
		scenario string
		layout   ColumnLayout
		schema   []format.SchemaElement
		expect   []format.SchemaElement
	}{
		{
			scenario: "the schema only contains a root column",
			layout:   ColumnLayout{},
			schema: []format.SchemaElement{
				{NumChildren: 0, Name: "$"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 0, Name: "$"},
			},
		},

		{
			scenario: "the schema has one child column and no specific layout",
			layout:   ColumnLayout{},
			schema: []format.SchemaElement{
				{NumChildren: 1, Name: "$"},
				{NumChildren: 0, Name: "A"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 1, Name: "$"},
				{NumChildren: 0, Name: "A"},
			},
		},

		{
			scenario: "the schema has one child column matching the layout",
			layout: ColumnLayout{
				{"A"},
			},
			schema: []format.SchemaElement{
				{NumChildren: 1, Name: "$"},
				{NumChildren: 0, Name: "A"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 1, Name: "$"},
				{NumChildren: 0, Name: "A"},
			},
		},

		{
			scenario: "the schema has one child column not matching the layout",
			layout: ColumnLayout{
				{"B"},
			},
			schema: []format.SchemaElement{
				{NumChildren: 1, Name: "$"},
				{NumChildren: 0, Name: "A"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 1, Name: "$"},
				{NumChildren: 0, Name: "A"},
			},
		},

		{
			scenario: "the schema has multiple top-level columns matching the layout",
			layout: ColumnLayout{
				{"A"}, {"B"}, {"C"},
			},
			schema: []format.SchemaElement{
				{NumChildren: 3, Name: "$"},
				{NumChildren: 0, Name: "A"},
				{NumChildren: 0, Name: "B"},
				{NumChildren: 0, Name: "C"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 3, Name: "$"},
				{NumChildren: 0, Name: "A"},
				{NumChildren: 0, Name: "B"},
				{NumChildren: 0, Name: "C"},
			},
		},

		{
			scenario: "the schema has multiple top-level columns not matching the layout",
			layout: ColumnLayout{
				{"C"}, {"B"}, {"A"},
			},
			schema: []format.SchemaElement{
				{NumChildren: 3, Name: "$"},
				{NumChildren: 0, Name: "A"},
				{NumChildren: 0, Name: "B"},
				{NumChildren: 0, Name: "C"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 3, Name: "$"},
				{NumChildren: 0, Name: "C"},
				{NumChildren: 0, Name: "B"},
				{NumChildren: 0, Name: "A"},
			},
		},

		{
			scenario: "the schema has nested columns matching the layout",
			layout: ColumnLayout{
				{"details", "last_name"},
				{"details", "first_name"},
				{"id"},
			},
			schema: []format.SchemaElement{
				{NumChildren: 2, Name: "$"},
				{NumChildren: 2, Name: "details"},
				{NumChildren: 0, Name: "last_name"},
				{NumChildren: 0, Name: "first_name"},
				{NumChildren: 0, Name: "id"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 2, Name: "$"},
				{NumChildren: 2, Name: "details"},
				{NumChildren: 0, Name: "last_name"},
				{NumChildren: 0, Name: "first_name"},
				{NumChildren: 0, Name: "id"},
			},
		},

		{
			scenario: "the schema has nested columns not matching the layout",
			layout: ColumnLayout{
				{"id"},
				{"details", "first_name"},
				{"details", "last_name"},
			},
			schema: []format.SchemaElement{
				{NumChildren: 2, Name: "$"},
				{NumChildren: 2, Name: "details"},
				{NumChildren: 0, Name: "last_name"},
				{NumChildren: 0, Name: "first_name"},
				{NumChildren: 0, Name: "id"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 2, Name: "$"},
				{NumChildren: 0, Name: "id"},
				{NumChildren: 2, Name: "details"},
				{NumChildren: 0, Name: "first_name"},
				{NumChildren: 0, Name: "last_name"},
			},
		},

		{
			scenario: "the schema has columns that are not declared in the layout",
			layout: ColumnLayout{
				{"C"}, {"B"},
			},
			schema: []format.SchemaElement{
				{NumChildren: 5, Name: "$"},
				{NumChildren: 0, Name: "A"},
				{NumChildren: 0, Name: "B"},
				{NumChildren: 0, Name: "C"},
				{NumChildren: 0, Name: "D"},
				{NumChildren: 0, Name: "E"},
			},
			expect: []format.SchemaElement{
				{NumChildren: 5, Name: "$"},
				{NumChildren: 0, Name: "C"},
				{NumChildren: 0, Name: "B"},
				{NumChildren: 0, Name: "A"},
				{NumChildren: 0, Name: "D"},
				{NumChildren: 0, Name: "E"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			schema := test.layout.orderedSchemaElements(test.schema)

			if !reflect.DeepEqual(schema, test.expect) {
				t.Errorf("\nexpect = %v\nschema = %v", test.expect, schema)
			}
		})
	}
}
