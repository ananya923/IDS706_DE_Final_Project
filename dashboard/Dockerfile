# Use official Python image
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements if you have one (optional for now)
# COPY requirements.txt .

# Install Streamlit and needed packages
RUN pip install streamlit pandas sqlalchemy pymysql

# Copy app files into container
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
