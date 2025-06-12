FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y g++ unixodbc-dev gnupg2 curl && \
    rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:10000"]