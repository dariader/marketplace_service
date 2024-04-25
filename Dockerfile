FROM python:3.10

WORKDIR /app
RUN mkdir data
RUN mkdir src
COPY ./data/ ./data
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN curl -o "data.xml" "http://export.admitad.com/ru/webmaster/websites/777011/products/export_adv_products/?user=bloggers_style&code=uzztv9z1ss&feed_id=21908&format=xml"
COPY config.py .
COPY main.py .
COPY ./src/ ./src/

CMD ["python", "./main.py"]