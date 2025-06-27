#!/usr/bin/env python3
"""
Simple test script to process the reviews dataset without Lambda functions
This will generate the required results for your assignment
"""
import json
import re
from collections import defaultdict

def simple_preprocess_text(text):
    """Simple text preprocessing without NLTK"""
    if not text or not isinstance(text, str):
        return ""
    
    # Convert to lowercase and remove special characters
    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    
    # Basic tokenization and stop word removal
    stop_words = {'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 
                  'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the', 
                  'to', 'was', 'will', 'with', 'i', 'me', 'my', 'we', 'our', 'you', 
                  'your', 'him', 'his', 'she', 'her', 'they', 'them', 'their', 'this'}
    
    tokens = [word for word in text.split() if word not in stop_words and len(word) > 2]
    return ' '.join(tokens)

def check_profanity(text):
    """Simple profanity check"""
    if not text:
        return False
    
    profane_words = {'damn', 'hell', 'crap', 'stupid', 'hate', 'terrible', 'awful', 
                     'worst', 'horrible', 'garbage', 'trash', 'shit', 'fuck', 'bitch'}
    
    text_lower = text.lower()
    return any(word in text_lower for word in profane_words)

def analyze_sentiment(text, overall_rating):
    """Simple sentiment analysis based on rating and text"""
    if overall_rating >= 4:
        return 'positive'
    elif overall_rating <= 2:
        return 'negative'
    else:
        return 'neutral'

def process_reviews():
    """Process the reviews dataset and generate results"""
    print("🚀 Starting review analysis...")
    
    # Initialize counters
    sentiment_counts = {'positive': 0, 'neutral': 0, 'negative': 0}
    profanity_count = 0
    user_profanity_counts = defaultdict(int)
    banned_users = []
    total_reviews = 0
    
    # Process the dataset
    try:
        with open('my_new_review.json', 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        review = json.loads(line.strip())
                        total_reviews += 1
                        
                        # Extract fields
                        reviewer_id = review.get('reviewerID', 'unknown')
                        summary = review.get('summary', '')
                        review_text = review.get('reviewText', '')
                        overall = review.get('overall', 3)
                        
                        # Preprocess text
                        processed_summary = simple_preprocess_text(summary)
                        processed_review = simple_preprocess_text(review_text)
                        
                        # Check for profanity
                        has_profanity = (check_profanity(summary) or 
                                       check_profanity(review_text) or 
                                       check_profanity(str(overall)))
                        
                        if has_profanity:
                            profanity_count += 1
                            user_profanity_counts[reviewer_id] += 1
                            
                            # Check if user should be banned (>3 unpolite reviews)
                            if (user_profanity_counts[reviewer_id] > 3 and 
                                reviewer_id not in [u['user_id'] for u in banned_users]):
                                banned_users.append({
                                    'user_id': reviewer_id,
                                    'unpolite_count': user_profanity_counts[reviewer_id]
                                })
                        
                        # Analyze sentiment
                        sentiment = analyze_sentiment(processed_review, overall)
                        sentiment_counts[sentiment] += 1
                        
                        # Progress indicator
                        if line_num % 10000 == 0:
                            print(f"   Processed {line_num:,} reviews...")
                            
                    except json.JSONDecodeError:
                        print(f"   Skipping invalid JSON on line {line_num}")
                        continue
                    except Exception as e:
                        print(f"   Error processing line {line_num}: {e}")
                        continue
    
    except FileNotFoundError:
        print("❌ Error: my_new_review.json not found. Please ensure the file is in the current directory.")
        return None
    
    # Generate results
    results = {
        "total_reviews": total_reviews,
        "positive_reviews": sentiment_counts['positive'],
        "neutral_reviews": sentiment_counts['neutral'], 
        "negative_reviews": sentiment_counts['negative'],
        "failed_profanity_check": profanity_count,
        "banned_users_count": len(banned_users),
        "banned_users": banned_users
    }
    
    return results

def main():
    """Main function"""
    print("📊 Review Processing Pipeline - Assignment 3")
    print("=" * 50)
    
    results = process_reviews()
    
    if results:
        print("\n✅ Analysis Complete!")
        print("=" * 50)
        print(f"📈 Results Summary:")
        print(f"   Total Reviews: {results['total_reviews']:,}")
        print(f"   Positive Reviews: {results['positive_reviews']:,} ({results['positive_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"   Neutral Reviews: {results['neutral_reviews']:,} ({results['neutral_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"   Negative Reviews: {results['negative_reviews']:,} ({results['negative_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"   Failed Profanity Check: {results['failed_profanity_check']:,} ({results['failed_profanity_check']/results['total_reviews']*100:.1f}%)")
        print(f"   Banned Users: {results['banned_users_count']}")
        
        if results['banned_users']:
            print(f"\n🚫 Banned Users:")
            for user in results['banned_users']:
                print(f"   - {user['user_id']} ({user['unpolite_count']} unpolite reviews)")
        
        # Save results to file
        with open('assignment_results_final.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Results saved to 'assignment_results_final.json'")
        print("🎯 This file contains all the required assignment results!")
        
    else:
        print("❌ Analysis failed. Please check the error messages above.")

if __name__ == "__main__":
    main()