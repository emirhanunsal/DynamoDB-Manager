import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key

# Function to read AWS credentials from a text file
def get_credentials(file_path='credentials.txt'):
    credentials = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                key, value = line.strip().split('=')
                credentials[key] = value
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
    except Exception as e:
        print(f"Error reading credentials: {e}")
    return credentials

# Load credentials
creds = get_credentials()

# Validate credentials
if 'aws_access_key_id' in creds and 'aws_secret_access_key' in creds and 'region' in creds:
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=creds['aws_access_key_id'],
        aws_secret_access_key=creds['aws_secret_access_key'],
        region_name=creds['region']
    )
else:
    print("Error: Missing required credentials in the file.")
    exit()

# 1. Create Table
def create_table():
    try:
        table_name = input("Enter the table name: ")
        partition_key = input("Enter the Partition Key name: ")
        key_type = input(f"Choose Partition Key type (S: string, N: number): ").upper()

        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': partition_key, 'KeyType': 'HASH'}  # Partition Key
            ],
            AttributeDefinitions=[
                {'AttributeName': partition_key, 'AttributeType': key_type}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print(f"Creating table {table_name}...")
        table.wait_until_exists()
        print(f"Table created: {table_name}")
    except ClientError as e:
        print("Table creation error:", e.response['Error']['Message'])

# 2. Add Item
def add_item():
    table_name = input("Which table do you want to add data to? ")
    partition_key = input("Enter the Partition Key name: ")
    table = dynamodb.Table(table_name)

    try:
        # Partition Key value
        key_value = input(f"Enter the value for {partition_key}: ")
        key_value = int(key_value) if key_value.isdigit() else key_value

        # Dynamically add attributes
        item = {partition_key: key_value}
        print("Add additional attributes. Enter 'q' to finish.")
        
        while True:
            attribute_name = input("Enter the name of the attribute to add: ")
            if attribute_name.lower() == 'q':  # Exit
                break
            attribute_value = input(f"Enter the value for {attribute_name}: ")
            
            # Check if the value is numeric
            if attribute_value.isdigit():
                attribute_value = int(attribute_value)
            
            # Add the attribute to the item dictionary
            item[attribute_name] = attribute_value

        # Add the item to DynamoDB
        table.put_item(Item=item)
        print(f"Item added: {item}")
    except ClientError as e:
        print("Error adding item:", e.response['Error']['Message'])

# 3. Delete Item
def delete_item():
    table_name = input("Which table do you want to delete data from? ")
    partition_key = input("Enter the Partition Key name: ")
    table = dynamodb.Table(table_name)

    try:
        key_value = input(f"Enter the value for {partition_key} to delete: ")
        key_value = int(key_value) if key_value.isdigit() else key_value

        response = table.delete_item(
            Key={partition_key: key_value}
        )
        print(f"Item deleted: {partition_key}={key_value}")
    except ClientError as e:
        print("Error deleting item:", e.response['Error']['Message'])

# 4. List Items
def list_items():
    table_name = input("Which table do you want to list data from? ")
    table = dynamodb.Table(table_name)

    try:
        response = table.scan()
        items = response.get('Items', [])
        print(f"Items in table {table_name}:")
        for item in items:
            print(item)
    except ClientError as e:
        print("Error listing items:", e.response['Error']['Message'])
        
        from boto3.dynamodb.conditions import Attr, Key

# 5. Filter Items
def filter_items():
    table_name = input("Which table do you want to filter data from? ")
    table = dynamodb.Table(table_name)

    try:
        print("Filter options:")
        print("1. Attribute equals a value")
        print("2. Attribute contains a substring")
        print("3. Attribute greater than a value")
        print("4. Attribute less than a value")
        choice = input("Select a filter type (1-4): ")

        attribute_name = input("Enter the attribute name to filter by: ")
        attribute_value = input(f"Enter the value for {attribute_name}: ")

        # Convert numeric values if applicable
        if attribute_value.isdigit():
            attribute_value = int(attribute_value)

        # Build the filter expression
        if choice == '1':
            filter_expression = Attr(attribute_name).eq(attribute_value)
        elif choice == '2':
            filter_expression = Attr(attribute_name).contains(attribute_value)
        elif choice == '3':
            filter_expression = Attr(attribute_name).gt(attribute_value)
        elif choice == '4':
            filter_expression = Attr(attribute_name).lt(attribute_value)
        else:
            print("Invalid filter type. Returning to the menu.")
            return

        # Scan the table with the filter expression
        response = table.scan(FilterExpression=filter_expression)
        items = response.get('Items', [])

        if items:
            print(f"Filtered items from table {table_name}:")
            for item in items:
                print(item)
        else:
            print("No matching items found.")

    except ClientError as e:
        print("Error filtering items:", e.response['Error']['Message'])


if __name__ == "__main__":
    while True:
        print("\n1. Create Table")
        print("2. Add Item")
        print("3. Delete Item")
        print("4. List Items")
        print("5. Filter Items")
        print("6. Exit")
        choice = input("Make your choice: ")

        if choice == '1':
            create_table()
        elif choice == '2':
            add_item()
        elif choice == '3':
            delete_item()
        elif choice == '4':
            list_items()
        elif choice == '5':
            filter_items()
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("Invalid choice, try again.")
