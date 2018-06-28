This is a tool to help you migrate your emails from your local Maildir mailbox to your Google Apps email or GMail.

It uses Python's mailbox module, so could easily be modified to handle other source formats.

mailbox2gmail uses a simple thread pool so it can do multiple uploads in parallel without needing to load all your emails into RAM first as many simpler scripts do. In my experience, throughput is limited by Google's servers. This script will also also retry on errors, which is vital if you have many emails to upload.

You will need Google's gdata library. In Debian, apt-get install python-gdata. Others, see https://code.google.com/p/gdata-python-client/
