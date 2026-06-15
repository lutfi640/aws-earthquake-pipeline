FROM public.ecr.aws/lambda/python:3.11

# 1. Copy requirements.txt ke dalam container
COPY requirements.txt .

# 2. Paksa install library LANGSUNG ke folder utama Lambda
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# 3. Copy kodingan lo
COPY scripts/transformers/ ${LAMBDA_TASK_ROOT}/transformers/
COPY scripts/utils/ ${LAMBDA_TASK_ROOT}/utils/

# 4. Set handler
CMD [ "transformers.transform_earthquake.lambda_handler" ]