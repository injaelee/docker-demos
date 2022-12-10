FROM python:alpine3.16


RUN ls -ltha

# copy the application config template
COPY app/ /demo-app/

# copy the python requirements
COPY ./requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r ./requirements.txt

# set up the default config
COPY config/template.config.json /app/env/default-config.json
COPY config/template.config.json /app/template/config.json

# the default command
ENTRYPOINT ["python", "/demo-app/app.py"]

# the default arguments to the ENTRYPOINT
# overridable
CMD ["--config", "/app/env/default-config.json"]