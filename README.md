# Overview
1. download ts files in list
2. merge ts files in one file
3. ffmpeg to convert ts file to mp4

# Example Usage:
```
python ts_to_mp4.py --working_dir <working_dir> --out_name <out_name> --template_url https://<host>/<path>/index
```
where
- `working_dir` is the working directory where the while is processed
- `out_name` is the output name of the mp4
- `template_url` is the url of where the ts should be downloaded. The origianl url should be in the form 
```
<template_url>0.ts
<template_url>1.ts
<template_url>2.ts
```

Examine the network activity to find the url pattern

so on 
# TODO
- Need to be able to handle rate limiting smarter
- Need to be able to handle retry smarter
- pylint
- Quality check
- add pytest (create example site)




- Try
  - Youtube
  - Vimeo
  - Tiktok
  - Twitter
  - Reddit

- Run in Docker
- Run in Kubernetes
- Check Performance
- Create different agent
- Refactor into classes
- Figure out how to host video for chromecast
- Figure out how to setup remote server
- Figure out how to queue things up
- FIgure out how to parse index file