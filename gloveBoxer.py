import json
import boto3

#upload DynamoDB entry for non paired pitches
def upload2raw(labels, target_object):
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    
    # Get the DynamoDB table
    table_name = 'raw-pitches-data'
    table = dynamodb.Table(table_name)
    
    # Get the DynamoDB table
    table_name = 'raw-pitches-data'
    table = dynamodb.Table(table_name)
    
    
    
    # Split object_key to get pitch info
    pitch_info = target_object.split("-")
    name_split= pitch_info[2].split(".")
    pitch_pitcher = name_split[0] 
    pitch_id = pitch_info[0] + "-" + pitch_info[1] + pitch_pitcher
    pitch_sequence = "initial" if pitch_info[0] == "i" else "final"
    sorter_id = pitch_info[1] + "-" + pitch_pitcher
    
    print("Detected labels:")
    
    for label in labels[:50]:
        if "glove" in label['Name'] or "Glove" in label["Name"] and label['Instances']:
            print(f"Label: {label['Name']}, Confidence: {label['Confidence']}, BoundingBox: {label['Instances'][0]['BoundingBox']}")
    
            # Item to save into DynamoDB raw pitches data
            item = {
                'pitchID': pitch_id,
                'name': target_object,
                'order': pitch_sequence,
                'pitcher' : pitch_pitcher,
                'gloveX': str(label['Instances'][0]['BoundingBox']['Left']),
                'gloveY': str(label['Instances'][0]['BoundingBox']['Top'])
            }
            
            item_sorted = {
                'pitcherPitchesID': pitch_sequence,
            }
            
            # Upload pitch into table
            try:
                response = table.put_item(Item=item)
                break
            except Exception as e:
                print(f"Ummm that shouldn't have happened... {e}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({'message': 'Error uploading entry into DynamoDB table'})
                }
            
                
def upload2sorted(labels, target_object):
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    
    # Get the DynamoDB table
    table_name = 'sorted-pitchers-pitches'
    table = dynamodb.Table(table_name)
    
    # Split object_key to get pitch info
    pitch_info = target_object.split("-")
    name_split= pitch_info[2].split(".")
    pitch_pitcher = name_split[0] 
    pitch_id = pitch_info[0] + "-" + pitch_info[1] + pitch_pitcher
    pitch_sequence = "initial" if pitch_info[0] == "i" else "final"
    sorter_id = pitch_info[1] + "-" + pitch_pitcher
    
    #for label in labels[:50]:
    #    if "glove" in label['Name'] or "Glove" in label["Name"] and label['Instances']:
     
    print(checkExistingSorted("f-260823006",'f-260823006'))
    
def checkExistingSorted(pitchId, partition_key_value):
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb')
    
    table_name = 'raw-pitches-data'
    
    try:
        # Get the DynamoDB table
        table = dynamodb.Table(table_name)
        print(partition_key_value)
        
        # Retrieve item by partition key value
        response = table.get_item(
            Key={
                'pitchID': {'S': partition_key_value}
            }
        )
        
        if 'Item' in response:
                item = response['Item']
                print("Item in response")
                return item
        else:
            print(f"Item not found with partition key value: {partition_key_value}")
            return None

    except Exception as e:
        print(f"Error retrieving item: {e}")
        return None
    
    
    
def search_labels(image):
    rekognition_client = boto3.client('rekognition')
    
    # Use Rekognition to detect labels in the image
    try:
        rekognition_response = rekognition_client.detect_labels(Image={'Bytes': image}, MaxLabels=50)
        labels = rekognition_response['Labels']
    except Exception as e:
        print(f"Error detecting labels in image: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Error detecting labels in image'})
        }
        
    return labels
    
def retrieve_image(bucket_name, object_key):
    s3_client = boto3.client('s3')
    
    # Get the image content from S3
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        image_content = response['Body'].read()
    except Exception as e:
        print(f"Error getting object {object_key} from bucket {bucket_name}: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Error getting image from S3'})
        }
    
    return image_content

def lambda_handler(event, context):
    # Define the S3 bucket name and object key
    bucket_name = "raw-pitches-source"
    object_key = event['Records'][0]['s3']['object']['key']
    
    ###table_sorted_name = 'sorted-pitchers-pitches'
    ###table_sorted = dynamodb.Table(table_sorted_name)
    
    #Call function to retrieve image from s3 bucket
    image_content = retrieve_image(bucket_name, object_key)
    
    #Call function to get labels from AWS Rekognition
    labels = search_labels(image_content)

    upload2raw(labels, object_key)
     
    upload2sorted(labels, object_key)           
    # Return success response
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Labels detected successfully'})
    }
