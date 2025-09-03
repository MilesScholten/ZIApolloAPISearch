setup:
	pip install -r requirements.txt

run:
	streamlit run app/streamlit_app.py

test:
	pytest -q
