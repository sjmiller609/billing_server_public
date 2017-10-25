# billing_server_public
scaling EC2 using Boto

This is the master/server code for the selenium bot solution.
This scale an image horizontally and provides some server logic for distributing data to the nodes.
Since I was the only contributor, I didn't really comment too much.

boto_utils.py is probably the most useful piece

EDIT: i would not use this now - i would recommend simplifying deployment, blacklisting, and other aws interaction using something like ansible
