"""Gunicorn launcher: one worker serializes pilot writes; port comes from Apps."""
import os

if __name__ == '__main__':
    port = int(os.environ['DATABRICKS_APP_PORT'])
    os.execvp('gunicorn', ['gunicorn', 'investigator.server:create_app()', '--bind', f'0.0.0.0:{port}', '--workers', '1', '--threads', '4', '--timeout', '900', '--access-logfile', '-'])
