using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            // Step 1: Create the DynamoDB client
            var client = new AmazonDynamoDBClient();
            string tableName = "table-name"; //table name

            Console.WriteLine("Scanning the table to fetch all records...");

            Dictionary<string, AttributeValue> lastEvaluatedKey = null; //used to fetch all records, dynamo db doesnt fetch all records at once
            List<Dictionary<string, AttributeValue>> allRecords = new List<Dictionary<string, AttributeValue>>();

            do
            {
                // Step 2: Scan the table to fetch all user records
                var scanRequest = new ScanRequest
                {
                    TableName = tableName,
                    ProjectionExpression = "primaryKeyID,anotherColumn", // add the column names which you want to query 
                    ExclusiveStartKey = lastEvaluatedKey

                };

                var scanResponse = await client.ScanAsync(scanRequest);

                var items = await client.ScanAsync(scanRequest);

                //This is the where condition in the query 
                var filteredItems = scanResponse.Items
         .Where(item => item.ContainsKey("anotherColumn") && item["anotherColumn"].BOOL == false).ToList(); // Filter where anotherColumn = false

                allRecords.AddRange(filteredItems);

                lastEvaluatedKey = scanResponse.LastEvaluatedKey;

            } while (lastEvaluatedKey != null && lastEvaluatedKey.Count > 0);



            Console.WriteLine($"Found {allRecords.Count} records to update.");

            foreach (var item in allRecords)
            {
                if (!item.ContainsKey("primaryKeyID")) continue; // Skip invalid items

                string primaryKeyID = item["primaryKeyID"].S; // Extract user ID

                var updateRequest = new UpdateItemRequest
                {
                    TableName = tableName,
                    Key = new Dictionary<string, AttributeValue>
            {
              { "primaryKeyID", new AttributeValue { S = primaryKeyID } } // Primary Key
                        },
                    UpdateExpression = "SET anotherColumn = :boolValue",
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
              { ":boolValue", new AttributeValue { BOOL = true } }, // Update: Set anotherColumn = true
                    
                        }
                };

                await client.UpdateItemAsync(updateRequest);
                Console.WriteLine($" Updated primaryKeyID: {primaryKeyID}");
            }

            Console.WriteLine("Bulk update completed!");
        }
        catch (Exception ex)
        {
        }
    }
}