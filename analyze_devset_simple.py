#!/usr/bin/env python3
"""
Simple analysis of reviews_devset.json without external dependencies
This gives us the basic results required for the assignment
"""
import json
import re
from collections import Counter

# Simple profanity word list (subset of what our full implementation uses)
PROFANITY_WORDS = {
    'damn', 'hell', 'shit', 'fuck', 'bitch', 'ass', 'bastard', 'crap',
    'piss', 'bloody', 'suck', 'sucks', 'stupid', 'idiot', 'moron',
    'dumb', 'hate', 'terrible', 'awful', 'worst', 'horrible', 'disgusting',
    'garbage', 'trash', 'worthless', 'pathetic', 'useless', 'annoying',
    'lame', 'dumbass', 'jackass', 'bullshit', 'goddamn', 'wtf', 'omg',
    'screw', 'screwed', 'screwing', 'scam', 'fake', 'fraud', 'ripoff',
    'rip-off', 'con', 'cheat', 'steal', 'stealing', 'robbed', 'robbery',
    'crappy'  # Found in the dataset
}

def simple_profanity_check(text):
    """Simple profanity detection using word matching"""
    if not text:
        return False, []
    
    text_lower = text.lower()
    # Remove punctuation for better matching
    clean_text = re.sub(r'[^\w\s]', ' ', text_lower)
    words = clean_text.split()
    
    profanity_found = []
    for word in words:
        if word in PROFANITY_WORDS:
            profanity_found.append(word)
    
    return len(profanity_found) > 0, profanity_found

def process_reviews():
    """Process the reviews_devset.json file"""
    print("🚀 Processing reviews_devset.json for Assignment 3")
    print("=" * 60)
    
    results = {
        'total_reviews': 0,
        'positive_reviews': 0,
        'neutral_reviews': 0, 
        'negative_reviews': 0,
        'profane_reviews': 0,
        'clean_reviews': 0,
        'user_profanity_counts': Counter(),
        'all_profanity_words': []
    }
    
    try:
        with open('/mnt/c/Users/vikho/PycharmProjects/DIC2025_Assignment3/data/reviews_devset.json', 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    review = json.loads(line.strip())
                    results['total_reviews'] += 1
                    
                    # Sentiment analysis based on overall rating
                    overall = review.get('overall', 3.0)
                    if overall >= 4.0:
                        results['positive_reviews'] += 1
                    elif overall <= 2.0:
                        results['negative_reviews'] += 1
                    else:
                        results['neutral_reviews'] += 1
                    
                    # Profanity check
                    summary = review.get('summary', '')
                    review_text = review.get('reviewText', '')
                    reviewer_id = review.get('reviewerID', 'unknown')
                    
                    summary_profane, summary_words = simple_profanity_check(summary)
                    text_profane, text_words = simple_profanity_check(review_text)
                    
                    contains_profanity = summary_profane or text_profane
                    
                    if contains_profanity:
                        results['profane_reviews'] += 1
                        results['user_profanity_counts'][reviewer_id] += 1
                        
                        all_words = summary_words + text_words
                        results['all_profanity_words'].extend(all_words)
                        
                        if line_num <= 10:  # Show first few for verification
                            print(f"   Found profanity in review {line_num}: {all_words}")
                            print(f"      Summary: {summary[:100]}...")
                    else:
                        results['clean_reviews'] += 1
                    
                    if line_num % 1000 == 0:
                        print(f"   Processed {line_num} reviews...")
                        
                except json.JSONDecodeError:
                    print(f"   ⚠️  Skipping invalid JSON on line {line_num}")
                    continue
    
    except FileNotFoundError:
        print("❌ File not found: data/reviews_devset.json")
        return
    
    # Generate results
    print("\n" + "=" * 60)
    print("📊 ASSIGNMENT 3 RESULTS REPORT")
    print("=" * 60)
    
    print(f"\n📈 SENTIMENT ANALYSIS RESULTS:")
    print(f"   Total reviews processed: {results['total_reviews']}")
    print(f"   ✅ Positive reviews: {results['positive_reviews']} ({results['positive_reviews']/results['total_reviews']*100:.1f}%)")
    print(f"   ➖ Neutral reviews: {results['neutral_reviews']} ({results['neutral_reviews']/results['total_reviews']*100:.1f}%)")
    print(f"   ❌ Negative reviews: {results['negative_reviews']} ({results['negative_reviews']/results['total_reviews']*100:.1f}%)")
    
    print(f"\n🚫 PROFANITY CHECK RESULTS:")
    print(f"   Clean reviews: {results['clean_reviews']} ({results['clean_reviews']/results['total_reviews']*100:.1f}%)")
    print(f"   Reviews that failed profanity check: {results['profane_reviews']} ({results['profane_reviews']/results['total_reviews']*100:.1f}%)")
    
    # User ban analysis
    banned_users = []
    for user_id, count in results['user_profanity_counts'].items():
        if count > 3:
            banned_users.append((user_id, count))
    
    print(f"\n👤 USER BAN ANALYSIS:")
    print(f"   Users with unpolite reviews: {len(results['user_profanity_counts'])}")
    print(f"   Users resulting in a ban (>3 unpolite): {len(banned_users)}")
    
    if banned_users:
        print(f"\n🚫 BANNED USERS:")
        for user_id, count in banned_users:
            print(f"   - {user_id}: {count} unpolite reviews")
    else:
        print(f"   ✅ No users meet the ban criteria")
    
    # Most common profanity
    if results['all_profanity_words']:
        profanity_counts = Counter(results['all_profanity_words'])
        print(f"\n🗣️  MOST COMMON PROFANITY WORDS:")
        for word, count in profanity_counts.most_common(10):
            print(f"   - '{word}': {count} occurrences")
    
    print("\n" + "=" * 60)
    print("✅ Analysis complete!")
    
    # Save summary
    summary = {
        'total_reviews': results['total_reviews'],
        'positive_reviews': results['positive_reviews'],
        'neutral_reviews': results['neutral_reviews'],
        'negative_reviews': results['negative_reviews'],
        'failed_profanity_check': results['profane_reviews'],
        'banned_users_count': len(banned_users),
        'banned_users': [{'user_id': uid, 'unpolite_count': count} for uid, count in banned_users]
    }
    
    with open('/mnt/c/Users/vikho/PycharmProjects/DIC2025_Assignment3/assignment_results.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"💾 Results saved to: assignment_results.json")

if __name__ == '__main__':
    process_reviews()