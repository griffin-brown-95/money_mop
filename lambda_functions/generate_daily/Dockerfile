FROM public.ecr.aws/lambda/python:3.11

# Copy function code
COPY app.py generate_daily.py requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command Lambda uses to start your app
CMD ["app.lambda_handler"]
