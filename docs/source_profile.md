# Source data profile

The following observations were made from the supplied files before implementing the transformations.

## Customers

- 793 rows and 793 unique Customer IDs
- 8 missing customer names
- 33 names containing numeric characters
- 103 phone values equal to `#ERROR!`
- 32 postal codes represented with four digits because the original leading zero was lost
- No malformed emails under a basic email-pattern check

## Products

- 1,851 rows
- 1,818 unique Product IDs
- 33 duplicate Product ID occurrences
- Duplicated IDs have consistent category and sub-category values
- Product name, state, and unit price can differ for the same Product ID

The product table must be consolidated before it is joined to orders.

## Orders

- 9,994 order-line rows
- 9,994 unique Row IDs
- 5,009 unique Order IDs
- No null values in the supplied order file
- No negative prices or non-positive quantities
- No discounts outside the 0-to-1 range
- No ship dates before order dates
- Years present: 2014 through 2017
- Total supplied profit: 278,417.03
- 44 Product IDs are missing from the product file, affecting 204 order lines
- All order Customer IDs are present in the customer file
