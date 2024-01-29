import hashlib


def generate_hash(web_url):
    if web_url is not None:
        return hashlib.sha256(web_url.encode('utf-8')).hexdigest()
    return None
