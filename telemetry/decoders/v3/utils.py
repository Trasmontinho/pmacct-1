import base64

def generate_content_from_raw(raw_file):
    with open(raw_file, "r") as fh:
        for line in fh:
            yield base64.b64decode(line.encode())
            

