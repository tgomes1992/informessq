import os
from dotenv import load_dotenv
from InformesLegais import create_app


load_dotenv()




if __name__ == "__main__":

    app = create_app()
    app.run(debug=True)