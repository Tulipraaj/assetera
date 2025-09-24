"""
Production entry point for the AssetEra Flask application
"""

from app import app, db

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    
    # For production, use a proper WSGI server like Gunicorn
    # gunicorn -w 4 -b 0.0.0.0:8000 run:app
    app.run(host='0.0.0.0', port=8000, debug=False)
