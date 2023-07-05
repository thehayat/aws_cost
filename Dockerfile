FROM python:3.9.15
WORKDIR /cost_metering
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
EXPOSE 8501
COPY . /cost_metering
CMD cd webapp
ENV PYTHONPATH "${PYTHONPATH}:/webapp/"
CMD streamlit run app.py