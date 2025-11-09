#!/usr/bin/env bash
set -e

# 1
yt-dlp "https://www.youtube.com/live/Qh6YznOLIzc?si=UHTYd2uHicKVowcO&t=8617" \
  --download-sections "*8617-8657" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_8617-8657.%(ext)s"

# 2
yt-dlp "https://www.youtube.com/live/Z6t-lyhUxYU?si=sqA7opmuneq8NH1O&t=643" \
  --download-sections "*643-683" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_643-683.%(ext)s"

# 3
yt-dlp "https://www.youtube.com/live/Qh6YznOLIzc?si=usXQQDb7k9dxe01n&t=8582" \
  --download-sections "*8582-8622" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_8582-8622.%(ext)s"

# 4
yt-dlp "https://www.youtube.com/live/Qh6YznOLIzc?si=0YChPvHlytMHbM-C&t=4081" \
  --download-sections "*4081-4121" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_4081-4121.%(ext)s"

# 5
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk?si=gLg4UIlJb4pQpDtP&t=6209" \
  --download-sections "*6209-6249" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_6209-6249.%(ext)s"

# 6
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk?si=5oHSAQpuy17Z1nPk&t=5123" \
  --download-sections "*5123-5163" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_5123-5163.%(ext)s"

# 7
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk?si=xdGEdUc59QsSfeNi&t=4978" \
  --download-sections "*4978-5018" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_4978-5018.%(ext)s"

# 8
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk?si=eu7O7eRMpgccdp54&t=4936" \
  --download-sections "*4936-4976" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_4936-4976.%(ext)s"

# 9
yt-dlp "https://www.youtube.com/live/T4wcS286Bjk?si=q4oSIP6AvzvlYlSL&t=2596" \
  --download-sections "*2596-2636" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_2596-2636.%(ext)s"

# 10
yt-dlp "https://www.youtube.com/live/beYpY_ME7AY?si=NgppDXFzoDUmML3I&t=5972" \
  --download-sections "*5972-6012" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_5972-6012.%(ext)s"

# 11
yt-dlp "https://www.youtube.com/live/Z6t-lyhUxYU?si=KkCZLdgEODb4dCgd&t=660" \
  --download-sections "*660-700" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_660-700.%(ext)s"

# 12
yt-dlp "https://www.youtube.com/watch?v=xW6q6Jcvgaw&t=3383s" \
  --download-sections "*3383-3423" \
  --force-keyframes-at-cuts \
  -S proto:https \
  -o "%(title)s_3383-3423.%(ext)s"