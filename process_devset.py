#!/usr/bin/env python3
"""
Process the reviews_devset.json file to test our Lambda functions
and generate the required results for the assignment
"""
import json
import sys
import os
from collections import Counter
from datetime import datetime

# Add the lambda function directories to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src/lambda_functions/preprocessing'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'src/lambda_functions/profanity_check'))

def load_reviews(file_path):
    """Load reviews from JSONL file"""
    reviews = []
    print(f"📖 Loading reviews from {file_path}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                review = json.loads(line.strip())
                reviews.append(review)
            except json.JSONDecodeError as e:
                print(f"⚠️  Error parsing line {line_num}: {e}")
                continue
    
    print(f"✅ Loaded {len(reviews)} reviews")
    return reviews

def process_reviews_with_functions(reviews):
    """Process reviews using our Lambda function logic"""
    print("🔄 Processing reviews with our Lambda functions...")
    
    # Import our function logic
    from lambda_function import preprocess_text, check_profanity_in_text
    
    results = {
        'total_reviews': 0,
        'positive_reviews': 0,
        'neutral_reviews': 0,
        'negative_reviews': 0,
        'profane_reviews': 0,
        'clean_reviews': 0,
        'profane_review_details': [],
        'processing_errors': 0
    }
    
    for i, review in enumerate(reviews):
        try:
            results['total_reviews'] += 1
            
            # Classify sentiment based on overall rating
            overall = review.get('overall', 3.0)
            if overall >= 4.0:
                results['positive_reviews'] += 1
                sentiment = 'positive'
            elif overall <= 2.0:
                results['negative_reviews'] += 1
                sentiment = 'negative'
            else:
                results['neutral_reviews'] += 1
                sentiment = 'neutral'
            
            # Process text fields
            summary = review.get('summary', '')
            review_text = review.get('reviewText', '')
            
            # Preprocess text
            if summary:
                preprocess_text(summary)
            if review_text:
                preprocess_text(review_text)
            
            # Check for profanity in all text fields
            summary_profanity = check_profanity_in_text(summary)
            review_profanity = check_profanity_in_text(review_text)
            
            contains_profanity = (
                summary_profanity['contains_profanity'] or 
                review_profanity['contains_profanity']
            )
            
            if contains_profanity:
                results['profane_reviews'] += 1
                
                # Store details for analysis
                profanity_words = list(set(
                    summary_profanity['profanity_words'] + 
                    review_profanity['profanity_words']
                ))
                
                results['profane_review_details'].append({
                    'reviewer_id': review.get('reviewerID', 'unknown'),
                    'asin': review.get('asin', 'unknown'),
                    'sentiment': sentiment,
                    'overall_rating': overall,
                    'profanity_words': profanity_words,
                    'summary': summary[:100] + '...' if len(summary) > 100 else summary,
                    'review_snippet': review_text[:200] + '...' if len(review_text) > 200 else review_text
                })
            else:
                results['clean_reviews'] += 1
                
            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"   Processed {i + 1}/{len(reviews)} reviews...")
                
        except Exception as e:
            results['processing_errors'] += 1
            print(f"⚠️  Error processing review {i}: {e}")
    
    return results

def analyze_banned_users(profane_reviews):
    """Analyze which users should be banned (>3 unpolite reviews)"""
    print("👤 Analyzing user ban status...")
    
    user_profanity_counts = Counter()
    
    for review in profane_reviews:
        user_profanity_counts[review['reviewer_id']] += 1
    
    banned_users = []
    for user_id, count in user_profanity_counts.items():
        if count > 3:
            banned_users.append({
                'reviewer_id': user_id,
                'unpolite_reviews': count
            })
    
    return banned_users, user_profanity_counts

def generate_report(results):
    """Generate the required assignment report"""
    print("\n" + "="*60)
    print("📊 ASSIGNMENT 3 RESULTS REPORT")
    print("="*60)
    
    print(f"\n📈 SENTIMENT ANALYSIS RESULTS:")
    print(f"   Total reviews processed: {results['total_reviews']}")
    print(f"   ✅ Positive reviews: {results['positive_reviews']} ({results['positive_reviews']/results['total_reviews']*100:.1f}%)")
    print(f"   ➖ Neutral reviews: {results['neutral_reviews']} ({results['neutral_reviews']/results['total_reviews']*100:.1f}%)")
    print(f"   ❌ Negative reviews: {results['negative_reviews']} ({results['negative_reviews']/results['total_reviews']*100:.1f}%)")
    
    print(f"\n🚫 PROFANITY CHECK RESULTS:")
    print(f"   Clean reviews: {results['clean_reviews']} ({results['clean_reviews']/results['total_reviews']*100:.1f}%)")
    print(f"   Reviews that failed profanity check: {results['profane_reviews']} ({results['profane_reviews']/results['total_reviews']*100:.1f}%)")
    
    # Analyze banned users
    banned_users, user_counts = analyze_banned_users(results['profane_review_details'])
    
    print(f"\n👤 USER BAN ANALYSIS:")
    print(f"   Users with unpolite reviews: {len(user_counts)}")
    print(f"   Users resulting in a ban (>3 unpolite): {len(banned_users)}")
    
    if banned_users:
        print(f"\n🚫 BANNED USERS:")
        for user in banned_users:
            print(f"   - {user['reviewer_id']}: {user['unpolite_reviews']} unpolite reviews")
    else:
        print(f"   No users meet the ban criteria (>3 unpolite reviews)")
    
    # Most common profanity words
    if results['profane_review_details']:
        all_profanity_words = []
        for review in results['profane_review_details']:
            all_profanity_words.extend(review['profanity_words'])
        
        profanity_counter = Counter(all_profanity_words)
        print(f"\n🗣️  MOST COMMON PROFANITY WORDS:")
        for word, count in profanity_counter.most_common(10):
            print(f"   - '{word}': {count} occurrences")
    
    if results['processing_errors'] > 0:
        print(f"\n⚠️  PROCESSING ERRORS: {results['processing_errors']}")
    
    print("\n" + "="*60)
    
    return {
        'positive_reviews': results['positive_reviews'],
        'neutral_reviews': results['neutral_reviews'], 
        'negative_reviews': results['negative_reviews'],
        'failed_profanity_check': results['profane_reviews'],
        'banned_users': len(banned_users),
        'banned_user_list': banned_users
    }

def main():
    """Main processing function"""
    print("🚀 Processing reviews_devset.json for Assignment 3")
    print("="*60)
    
    # File path
    devset_path = 'data/reviews_devset.json'
    
    if not os.path.exists(devset_path):
        print(f"❌ File not found: {devset_path}")
        print("   Please ensure reviews_devset.json is in the data/ directory")
        return 1
    
    try:
        # Load reviews
        reviews = load_reviews(devset_path)
        
        if not reviews:
            print("❌ No reviews loaded")
            return 1
        
        # Process reviews
        results = process_reviews_with_functions(reviews)
        
        # Generate report
        summary = generate_report(results)
        
        # Save results to file
        output_file = 'assignment_3_results.json'
        with open(output_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'dataset': 'reviews_devset.json',
                'total_reviews': results['total_reviews'],
                'sentiment_analysis': {
                    'positive': results['positive_reviews'],
                    'neutral': results['neutral_reviews'],
                    'negative': results['negative_reviews']
                },
                'profanity_check': {
                    'clean_reviews': results['clean_reviews'],
                    'failed_profanity_check': results['profane_reviews']
                },
                'user_bans': {
                    'banned_users_count': summary['banned_users'],
                    'banned_users': summary['banned_user_list']
                },
                'sample_profane_reviews': results['profane_review_details'][:5]  # First 5 for review
            }, f, indent=2)
        
        print(f"\n💾 Detailed results saved to: {output_file}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error processing reviews: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())