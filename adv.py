import webbrowser

# Loom video embed code
loom_embed_code = """
<!DOCTYPE html>
<html>
<head>
    <title>Loom Video</title>
</head>
<body>
    <iframe width="560" height="315" src="https://www.loom.com/embed/your-video-url" frameborder="0" allowfullscreen></iframe>
</body>
</html>
"""

# Write the embed code to an HTML file
with open('loom_video_embed.html', 'w') as file:
    file.write(loom_embed_code)

# Open the HTML file in the default web browser
webbrowser.open('loom_video_embed.html')
