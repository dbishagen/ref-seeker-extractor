package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

// -- data structures

type INDdbObject struct {
	database   string
	entityType string
	attributes []string
}

type IND struct {
	left    INDdbObject
	right   INDdbObject
	id      string
	indType string
}

var inclusionDependencies = make(map[string]IND, 0)

type parsingType int

const (
	REFERENCES parsingType = iota
	MAXIMAL_INCLUSION_DEPENDENCIES
)

var schemaINDParsingKeys = map[string]string{
	"database_left":    "foreignkey_database_type",
	"entitytype_left":  "foreignkey_datastorage",
	"attributes_left":  "foreignkey_attributes",
	"database_right":   "primarykey_database_type",
	"entitytype_right": "primarykey_datastorage",
	"attributes_right": "primarykey_attributes",
}
var schemaINDParsingKeysMaximal = map[string]string{
	"database_left":    "child_server_type",
	"entitytype_left":  "child_datastorage_name",
	"attributes_left":  "child_attribute_names",
	"database_right":   "parent_server_type",
	"entitytype_right": "parent_datastorage_name",
	"attributes_right": "parent_attribute_names",
}

// -- functions

func getSplittedAttributes(attributes string) []string {
	// Split the attributes by comma and remove whitespace
	attributesList := strings.Split(attributes, ",")
	for i := 0; i < len(attributesList); i++ {
		attributesList[i] = strings.TrimSpace(attributesList[i])
	}
	return attributesList
}

func parseReferenceData(objectList []any, indType string, pType parsingType) error {

	var mapingKeys map[string]string
	switch pType {
	case REFERENCES:
		mapingKeys = schemaINDParsingKeys
	case MAXIMAL_INCLUSION_DEPENDENCIES:
		mapingKeys = schemaINDParsingKeysMaximal
	default:
		return fmt.Errorf("unknown parsing type")
	}

	for _, obj := range objectList {

		dataMap := obj.(map[string]any)

		indDbObjectLeft := INDdbObject{
			database:   dataMap[mapingKeys["database_left"]].(string),
			entityType: dataMap[mapingKeys["entitytype_left"]].(string),
			attributes: getSplittedAttributes(dataMap[mapingKeys["attributes_left"]].(string)),
		}
		//sort.Strings(indDbObjectLeft.attributes)
		indDbObjectRight := INDdbObject{
			database:   dataMap[mapingKeys["database_right"]].(string),
			entityType: dataMap[mapingKeys["entitytype_right"]].(string),
			attributes: getSplittedAttributes(dataMap[mapingKeys["attributes_right"]].(string)),
		}
		//sort.Strings(indDbObjectRight.attributes)
		ind := IND{
			left:    indDbObjectLeft,
			right:   indDbObjectRight,
			id:      indDbObjectLeft.database + "." + indDbObjectLeft.entityType + ".[" + strings.Join(indDbObjectLeft.attributes, ",") + "]->" + indDbObjectRight.database + "." + indDbObjectRight.entityType + ".[" + strings.Join(indDbObjectRight.attributes, ",") + "]",
			indType: indType,
		}

		inclusionDependencies[ind.id] = ind
	}
	return nil
}

func parseSchemaData(data io.Reader) error {
	// Decode the JSON file into a map
	var result map[string]any
	if err := json.NewDecoder(data).Decode(&result); err != nil {
		return err
	}

	// Get the schmemaobject from the map
	schema := result["json_schema"].([]any)[0].(map[string]any)

	// Check if the schema object is empty
	if len(schema) == 0 {
		return fmt.Errorf("schema object is empty")
	}

	// Iterate over the schema objects
	// These are the following:
	//   1. implicite_refences
	//   2. explicite_refences
	//   3. primary_keys
	//   4. databases (database schema)
	//   5. maximal_inclusion_dependencies
	for key, value := range schema {

		//fmt.Println("Key: ", key)
		schemaObjectList := value.([]any)

		// -- Parse implicite references
		if key == "implicite_refences" {
			if err := parseReferenceData(schemaObjectList, "implicite_refences", REFERENCES); err != nil {
				return fmt.Errorf("error parsing implicite references: %v", err)
			}
		}
		// -- Parse explicite references
		if key == "explicite_refences" {
			if err := parseReferenceData(schemaObjectList, "explicite_refences", REFERENCES); err != nil {
				return fmt.Errorf("error parsing explicite references: %v", err)
			}
		}
		// -- Parse maximal inclusion dependencies
		if key == "maximal_inclusion_dependencies" {
			if err := parseReferenceData(schemaObjectList, "maximal_inclusion_dependencies", MAXIMAL_INCLUSION_DEPENDENCIES); err != nil {
				return fmt.Errorf("error parsing maximal inclusion dependencies: %v", err)
			}
		}
	}

	return nil
}

func splitMaximalInclusionDependencies() {
	// Iterate over the inclusion dependencies
	for _, value := range inclusionDependencies {
		// Check if the type is maximal inclusion dependencies and if the left attributes have more than one attribute
		if value.indType == "maximal_inclusion_dependencies" && len(value.left.attributes) > 1 {
			// Loop over the left attributes
			for i := 0; i < len(value.left.attributes); i++ {
				// Create a new ID for this attribute
				id := value.left.database + "." + value.left.entityType + ".[" + value.left.attributes[i] + "]->" + value.right.database + "." + value.right.entityType + ".[" + value.right.attributes[i] + "]"
				// Check if the ID already exists
				if _, exists := inclusionDependencies[id]; !exists {
					// Create a new IND object for each attribute
					left := value.left
					right := value.right
					left.attributes = []string{value.left.attributes[i]}
					right.attributes = []string{value.right.attributes[i]}
					newIND := IND{
						left:    left,
						right:   right,
						id:      id,
						indType: "parsed_from_maximal_inclusion_dependencies",
					}
					inclusionDependencies[id] = newIND
				}
			}
		}
	}
}

func compareWith(foundIndMap map[string]IND, inclusionDependenciesFile string) error {

	fmt.Printf("\n## Found INDs Statistics ##\n")

	file, err := os.Open(inclusionDependenciesFile)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)

	existingIds := make([]string, 0)

	for scanner.Scan() {
		line := scanner.Text()
		// Check if the line starts with a comment
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		// remove all whitespace
		line = strings.ReplaceAll(line, " ", "")
		line = strings.ReplaceAll(line, "\t", "")
		line = strings.ReplaceAll(line, "\n", "")
		line = strings.ReplaceAll(line, "\r", "")
		// Check if the line is empty
		if line == "" {
			continue
		}
		existingIds = append(existingIds, line)
	}

	if err := file.Close(); err != nil {
		fmt.Printf("Error closing file %s: %v\n", inclusionDependenciesFile, err)
	}

	falseNegatives := 0
	truePositives := 0
	for _, existingInd := range existingIds {
		_, exists := foundIndMap[existingInd]
		if !exists {
			fmt.Printf("False negative (IND not found): %s\n", existingInd)
			falseNegatives++
		} else {
			truePositives++
		}
	}

	falsePositives := 0
	for key := range foundIndMap {
		if !slices.Contains(existingIds, key) {
			fmt.Printf("False positive (IND does not exist): %s\n", key)
			falsePositives++
		}
	}

	fmt.Printf("\nFalse negatives: %d\n", falseNegatives)
	fmt.Printf("False positives: %d\n", falsePositives)
	fmt.Printf("True positives: %d\n", truePositives)

	precision := float64(truePositives) / float64(truePositives+falsePositives)
	recall := float64(truePositives) / float64(truePositives+falseNegatives)
	fmt.Printf("\nPrecision: %.2f\n", precision)
	fmt.Printf("Recall: %.2f\n", recall)
	// Calculate F1 score
	f1Score := 2 * (precision * recall) / (precision + recall)
	fmt.Printf("F1 score: %.2f\n\n", f1Score)

	return nil
}

func printFoundInds(foundIndMap map[string]IND) {
	fmt.Printf("\n## Found INDs ##\n")
	for key := range foundIndMap {
		fmt.Println(key)
	}
}

func main() {

	indFilePathPtr := flag.String("i", "", "File containing the INDs to compare with")
	schemaFilePathPtr := flag.String("s", "", "Optional, a file containing the schema to parse")
	dbConfigFilePathPtr := flag.String("d", "", "A file containing the database configuration for refseeker")
	serverURLPtr := flag.String("u", "http://localhost:8001/", "The URL of the refseeker server")
	writeToFilePtr := flag.Bool("w", false, "Write the output to a file")
	printFoundIndsPtr := flag.Bool("p", false, "Print the found INDs")

	flag.Parse()

	serverURL = *serverURLPtr

	seenFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		seenFlags[f.Name] = true
	})

	if !seenFlags["i"] {
		fmt.Println("Please provide the file path using -i flag")
		flag.Usage()
		os.Exit(1)
	}

	if !seenFlags["d"] {
		fmt.Println("Please provide the database configuration file path using -d flag")
		flag.Usage()
		os.Exit(1)
	}

	var schemaData io.Reader
	// if a schema file is provided, use it
	if seenFlags["s"] {
		schema, err := os.ReadFile(*schemaFilePathPtr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading schema file: %v\n", err)
			os.Exit(1)
		}
		schemaData = bytes.NewReader(schema)
		// if no schema file is provided, extract the schema
	} else {

		// Get the database configuration
		dbConfig, err := os.ReadFile(*dbConfigFilePathPtr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading database configuration file: %v\n", err)
			os.Exit(1)
		}
		schema, err := getSchema(dbConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting schema: %v\n", err)
			os.Exit(1)
		}
		schemaData = bytes.NewReader(schema)
		if writeToFilePtr != nil && *writeToFilePtr {
			err := os.WriteFile("schema.json", schema, 0644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error writing schema file: %v\n", err)
				os.Exit(1)
			}
		}
	}

	// Parse the schema data
	err := parseSchemaData(schemaData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing schema data: %v\n", err)
		os.Exit(1)
	}

	// Split the maximal inclusion dependencies
	splitMaximalInclusionDependencies()

	// get a map with without the maximal inclusion dependencies and neo4j internal ids
	indMap := make(map[string]IND)

	for key, value := range inclusionDependencies {
		// Check if the type is  maximal inclusion dependencies
		if len(value.left.attributes) > 1 {
			continue
		}
		// Check if the left and right attributes are not in the ignore list
		ignoreList := []string{"startNodeElementId", "endNodeElementId", "elementID"}
		if slices.Contains(ignoreList, value.left.attributes[0]) || slices.Contains(ignoreList, value.right.attributes[0]) {
			continue
		}
		indMap[key] = value
	}

	// Print the found inclusion dependencies if the flag is set
	if printFoundIndsPtr != nil && *printFoundIndsPtr {
		printFoundInds(indMap)
	}

	// Compare with the found inclusion dependencies
	if err := compareWith(indMap, *indFilePathPtr); err != nil {
		fmt.Fprintf(os.Stderr, "Error comparing with found inclusion dependencies: %v\n", err)
		os.Exit(1)
	}
}
