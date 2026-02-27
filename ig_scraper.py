#!/usr/bin/env python3
"""Instagram profile scraper using Instaloader. Returns JSON to stdout."""
import sys
import json
import signal

# Timeout handler
def timeout_handler(signum, frame):
    raise TimeoutError("Script timeout")

try:
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(60)
except (AttributeError, ValueError):
    pass  # Windows doesn't support SIGALRM

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: ig_scraper.py <username>"}))
        sys.exit(1)
    
    username = sys.argv[1].lstrip('@')
    
    import instaloader
    from instaloader import Instaloader, Profile
    
    L = Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True,
        max_connection_attempts=1,  # Don't retry on failure
        request_timeout=15
    )
    
    # Disable the automatic retry/wait on 429
    L.context._rate_controller = None
    
    try:
        profile = Profile.from_username(L.context, username)
        
        result = {
            "username": profile.username,
            "full_name": profile.full_name,
            "biography": profile.biography,
            "followers": profile.followers,
            "following": profile.followees,
            "posts_count": profile.mediacount,
            "is_private": profile.is_private,
            "is_verified": profile.is_verified,
            "profile_pic_url": profile.profile_pic_url,
            "external_url": profile.external_url,
            "posts": []
        }
        
        # Get recent posts (up to 12) only if public
        if not profile.is_private:
            count = 0
            try:
                for post in profile.get_posts():
                    if count >= 12:
                        break
                    post_data = {
                        "id": post.shortcode,
                        "url": f"https://www.instagram.com/p/{post.shortcode}/",
                        "title": (post.caption or "Post")[:100],
                        "thumbnail": post.url,
                        "likes": post.likes,
                        "comments": post.comments,
                        "views": post.video_view_count or 0,
                        "is_video": post.is_video,
                        "upload_date": post.date_utc.isoformat() if post.date_utc else None
                    }
                    result["posts"].append(post_data)
                    count += 1
            except Exception:
                pass  # If posts fail, at least return profile data
        
        print(json.dumps(result))
        
    except instaloader.exceptions.ProfileNotExistsException:
        print(json.dumps({"error": f"Profile '{username}' does not exist"}))
        sys.exit(1)
    except instaloader.exceptions.ConnectionException as e:
        err_str = str(e)
        if "429" in err_str:
            print(json.dumps({"error": "rate_limited", "message": "Instagram rate limit (429). Try again later."}))
        else:
            print(json.dumps({"error": f"Connection error: {err_str}"}))
        sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)

if __name__ == "__main__":
    main()
