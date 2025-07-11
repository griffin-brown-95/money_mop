from lambda_functions.generate_daily.generate_daily import generate_daily_data

def lambda_handler(event, context):
    try:
        generate_daily_data()
        return {
            'statusCode': 200,
            'body': 'Daily data generation successful.'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }