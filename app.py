from flask import Flask
from dotenv import load_dotenv

app = Flask(__name__)

@app.route("/")
def index():
    return "Repository pattern with Flask + Supabase"

if __name__ == "__main__":
    app.run(debug=True)