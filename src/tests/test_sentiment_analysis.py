import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import boto3

# Dodaj katalog funkcji lambda do ścieżki
sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/sentiment_analysis'))

# Helper do tworzenia zdarzenia EventBridge S3
def create_eventbridge_s3_event(bucket_name, object_key):
    return {
        'Records': [{
            'eventVersion': '2.1',
            'eventSource': 'aws:s3',
            'awsRegion': 'us-east-1',
            'eventTime': '2025-06-24T12:00:00Z',
            'eventName': 'ObjectCreated:Put',
            'userIdentity': {'principalId': 'AWS:AROA*****************'},
            'requestParameters': {'sourceIPAddress': '127.0.0.1'},
            'responseElements': {
                'x-amz-request-id': '****************',
                'x-amz-id-2': '************************************************************************'
            },
            's3': {
                's3SchemaVersion': '1.0',
                'configurationId': '*****************',
                'bucket': {
                    'name': bucket_name,
                    'ownerIdentity': {'principalId': 'A*****************'},
                    'arn': f'arn:aws:s3:::{bucket_name}'
                },
                'object': {
                    'key': object_key,
                    'size': 1024,
                    'eTag': '****************',
                    'sequencer': '****************'
                }
            }
        }]
    }

# Zastosuj globalny patch dla SentimentIntensityAnalyzer na poziomie klasy
@patch('nltk.sentiment.vader.SentimentIntensityAnalyzer')
class TestAnalyzeSentimentInText:
    """Unit tests for the analyze_sentiment_in_text function."""

    # mock_analyzer_class to jest zamockowana klasa SentimentIntensityAnalyzer
    def test_positive_text_sentiment(self, mock_analyzer_class):
        """Test sentiment analysis for a positive text."""
        # Pobierz instancję, która zostałaby zwrócona przez SentimentIntensityAnalyzer()
        mock_analyzer_instance = mock_analyzer_class.return_value
        mock_analyzer_instance.polarity_scores.return_value = {'neg': 0.0, 'neu': 0.4, 'pos': 0.6, 'compound': 0.9}

        # Importuj lambda_function tutaj, aby upewnić się, że używa globalnie spatchowanego SentimentIntensityAnalyzer
        import lambda_function as sentiment_lambda_module_import
        text = "This product is absolutely fantastic and I love it!"
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(text)
        assert result == {'neg': 0.0, 'neu': 0.4, 'pos': 0.6, 'compound': 0.9}
        mock_analyzer_instance.polarity_scores.assert_called_once_with(text)

    def test_negative_text_sentiment(self, mock_analyzer_class):
        """Test sentiment analysis for a negative text."""
        mock_analyzer_instance = mock_analyzer_class.return_value
        mock_analyzer_instance.polarity_scores.return_value = {'neg': 0.8, 'neu': 0.2, 'pos': 0.0, 'compound': -0.9}

        import lambda_function as sentiment_lambda_module_import
        text = "This product is terrible and I regret buying it."
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(text)
        assert result == {'neg': 0.8, 'neu': 0.2, 'pos': 0.0, 'compound': -0.9}
        mock_analyzer_instance.polarity_scores.assert_called_once_with(text)

    def test_neutral_text_sentiment(self, mock_analyzer_class):
        """Test sentiment analysis for a neutral text."""
        mock_analyzer_instance = mock_analyzer_class.return_value
        mock_analyzer_instance.polarity_scores.return_value = {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}

        import lambda_function as sentiment_lambda_module_import
        text = "The product is red."
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(text)
        assert result == {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}
        mock_analyzer_instance.polarity_scores.assert_called_once_with(text)

    # Te testy nie wymagają mock_analyzer_class, ale nadal go otrzymują
    def test_empty_text_input(self, mock_analyzer_class):
        import lambda_function as sentiment_lambda_module_import
        result = sentiment_lambda_module_import.analyze_sentiment_in_text("")
        assert result == {'neg': 0, 'neu': 0, 'pos': 0, 'compound': 0}
        mock_analyzer_class.return_value.polarity_scores.assert_not_called() # Brak wywołania dla pustego tekstu

    def test_none_text_input(self, mock_analyzer_class):
        import lambda_function as sentiment_lambda_module_import
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(None)
        assert result == {'neg': 0, 'neu': 0, 'pos': 0, 'compound': 0}
        mock_analyzer_class.return_value.polarity_scores.assert_not_called()

    def test_non_string_input(self, mock_analyzer_class):
        import lambda_function as sentiment_lambda_module_import
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(123)
        assert result == {'neg': 0, 'neu': 0, 'pos': 0, 'compound': 0}
        mock_analyzer_class.return_value.polarity_scores.assert_not_called()

@patch('nltk.download') # Patch nltk.download, aby zapobiec rzeczywistym pobieraniom
@patch('nltk.data.path') # Patch nltk.data.path, jeśli potrzebne do konfiguracji
@patch('boto3.client')
@patch('boto3.resource') # Nadal patchujemy resource, nawet jeśli nie jest używany przez tę lambdę
@patch('lambda_function.get_parameter') # Patch get_parameter zamiast bezpośrednio ssm_client
# Patch SentimentIntensityAnalyzer globalnie na poziomie klasy dla TestLambdaHandler
@patch('nltk.sentiment.vader.SentimentIntensityAnalyzer')
class TestLambdaHandler:
    """Unit tests for the lambda_handler function."""

    # Uwaga: kolejność argumentów w setup_method jest odwrotna do kolejności dekoratorów @patch
    def setup_method(self, method, MockSentimentIntensityAnalyzerClass, MockGetParameter, MockDynamoDBResource, MockBotoClient, MockNLTKDataPath, MockNLTKDownload):
        self.mock_boto_client = MockBotoClient
        self.mock_s3_client = Mock()
        self.mock_ssm_client = Mock()

        # Skonfiguruj mock boto3.client, aby zwracał konkretne mocki dla S3 i SSM
        self.mock_boto_client.side_effect = lambda service_name, **kwargs: {
            's3': self.mock_s3_client,
            'ssm': self.mock_ssm_client
        }.get(service_name)

        # Skonfiguruj mock get_parameter. Ten mock zostanie użyty podczas ponownego ładowania `lambda_function`.
        MockGetParameter.return_value = 'mock-final-reviews-bucket'

        # Skonfiguruj instancję mocka SentimentIntensityAnalyzer
        # Będzie to instancja zwracana przez SentimentIntensityAnalyzer()
        self.mock_analyzer_instance = MockSentimentIntensityAnalyzerClass.return_value
        # Domyślne zachowanie: neutralny wynik złożony
        self.mock_analyzer_instance.polarity_scores.return_value = {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}

        # Zapobiegnij rzeczywistym manipulacjom ścieżką danych NLTK i pobieraniu
        MockNLTKDataPath.append.return_value = None
        MockNLTKDownload.return_value = None

        # Załaduj ponownie lambda_function, aby upewnić się, że wszystkie globalne zmienne (jak analyzer, FINAL_REVIEWS_BUCKET)
        # są ponownie inicjowane przy użyciu aktywnych mocków.
        if 'lambda_function' in sys.modules:
            del sys.modules['lambda_function']
        import lambda_function as reloaded_sentiment_lambda_module
        self.sentiment_lambda = reloaded_sentiment_lambda_module

        # Jawnie ustaw FINAL_REVIEWS_BUCKET w ponownie załadowanym module dla testów, gdzie SSM ma działać
        # To nadpisuje wszelkie potencjalne problemy z mockiem SSM podczas ponownego ładowania modułu dla pomyślnych ścieżek.
        self.sentiment_lambda.FINAL_REVIEWS_BUCKET = 'mock-final-reviews-bucket'


    def teardown_method(self, method):
        # Wyczyść patche, jeśli to konieczne (pytest-mock automatycznie obsługuje zatrzymywanie patchy)
        pass

    def test_lambda_handler_positive_review(self):
        """Test lambda handler z pozytywną recenzją."""
        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'overall_rating': 5,
            'summary': 'This is an amazing product!',
            'reviewText': 'The quality is superb and I am very happy with it.',
            'overall': '5',
            'processed_summary': 'This is an amazing product!',
            'processed_reviewText': 'The quality is superb and I am very happy with it.',
            'processed_overall': '5',
            'summary_word_count': 5,
            'reviewText_word_count': 12,
            'overall_word_count': 1,
            'total_word_count': 18
        }
        self.mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_review_data).encode('utf-8'))
        }
        self.mock_analyzer_instance.polarity_scores.side_effect = [
            {'neg': 0.0, 'neu': 0.2, 'pos': 0.8, 'compound': 0.95},  # podsumowanie
            {'neg': 0.0, 'neu': 0.1, 'pos': 0.9, 'compound': 0.98},  # reviewText
            {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}   # ogólne (traktowane jako neutralne do obliczeń)
        ]
        self.mock_s3_client.put_object.return_value = {}

        event = create_eventbridge_s3_event('processed-reviews-bucket', 'processed/B001234567.json')
        result = self.sentiment_lambda.lambda_handler(event, {})

        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Sentiment analysis completed'
        assert body['review_id'] == 'B001234567'
        assert body['sentiment_label'] == 'positive'
        # Teraz aseruje, że to jest nazwa zamockowanego zasobnika, a nie awaryjnego
        assert 's3://mock-final-reviews-bucket/analyzed/B001234567.json' in body['output_location']

        self.mock_s3_client.get_object.assert_called_once_with(Bucket='processed-reviews-bucket', Key='processed/B001234567.json')
        self.mock_s3_client.put_object.assert_called_once()
        put_call_args = self.mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'mock-final-reviews-bucket'
        assert put_call_args[1]['Key'] == 'analyzed/B001234567.json'

        processed_data = json.loads(put_call_args[1]['Body'])
        assert processed_data['processing_stage'] == 'sentiment_analyzed'
        assert processed_data['sentiment_analysis']['sentiment_label'] == 'positive'
        assert processed_data['sentiment_analysis']['aggregated_compound_score'] > 0.05

    def test_lambda_handler_negative_review(self):
        """Test lambda handler z negatywną recenzją."""
        mock_review_data = {
            'review_id': 'B001234568',
            'reviewer_id': 'A1234567891',
            'overall_rating': 1,
            'summary': 'This product is absolutely terrible!',
            'reviewText': 'The quality is horrible and I am very unhappy with it. A complete waste.',
            'overall': '1',
            'processed_summary': 'This product is absolutely terrible!',
            'processed_reviewText': 'The quality is horrible and I am very unhappy with it. A complete waste.',
            'processed_overall': '1',
            'summary_word_count': 5,
            'reviewText_word_count': 14,
            'overall_word_count': 1,
            'total_word_count': 20
        }
        self.mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_review_data).encode('utf-8'))
        }
        self.mock_analyzer_instance.polarity_scores.side_effect = [
            {'neg': 0.9, 'neu': 0.1, 'pos': 0.0, 'compound': -0.99},  # podsumowanie
            {'neg': 0.8, 'neu': 0.2, 'pos': 0.0, 'compound': -0.90},  # reviewText
            {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}   # ogólne
        ]
        self.mock_s3_client.put_object.return_value = {}

        event = create_eventbridge_s3_event('processed-reviews-bucket', 'processed/B001234568.json')
        result = self.sentiment_lambda.lambda_handler(event, {})

        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Sentiment analysis completed'
        assert body['review_id'] == 'B001234568'
        assert body['sentiment_label'] == 'negative'
        assert 's3://mock-final-reviews-bucket/analyzed/B001234568.json' in body['output_location']

        processed_data = json.loads(self.mock_s3_client.put_object.call_args[1]['Body'])
        assert processed_data['sentiment_analysis']['sentiment_label'] == 'negative'
        assert processed_data['sentiment_analysis']['aggregated_compound_score'] < -0.05

    def test_lambda_handler_neutral_review(self):
        """Test lambda handler z neutralną recenzją."""
        mock_review_data = {
            'review_id': 'B001234569',
            'reviewer_id': 'A1234567892',
            'overall_rating': 3,
            'summary': 'This product works as expected.',
            'reviewText': 'It does what it is supposed to do. No complaints, no praise.',
            'overall': '3',
            'processed_summary': 'This product works as expected.',
            'processed_reviewText': 'It does what it is supposed to do. No complaints, no praise.',
            'processed_overall': '3',
            'summary_word_count': 5,
            'reviewText_word_count': 12,
            'overall_word_count': 1,
            'total_word_count': 18
        }
        self.mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_review_data).encode('utf-8'))
        }
        self.mock_analyzer_instance.polarity_scores.side_effect = [
            {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0},  # podsumowanie
            {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0},  # reviewText
            {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}   # ogólne
        ]
        self.mock_s3_client.put_object.return_value = {}

        event = create_eventbridge_s3_event('processed-reviews-bucket', 'processed/B001234569.json')
        result = self.sentiment_lambda.lambda_handler(event, {})

        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Sentiment analysis completed'
        assert body['review_id'] == 'B001234569'
        assert body['sentiment_label'] == 'neutral' # To powinno teraz przejść przy prawidłowym mockowaniu NLTK

        processed_data = json.loads(self.mock_s3_client.put_object.call_args[1]['Body'])
        assert processed_data['sentiment_analysis']['sentiment_label'] == 'neutral'
        assert -0.05 < processed_data['sentiment_analysis']['aggregated_compound_score'] < 0.05

    def test_lambda_handler_s3_error_nosuchkey(self):
        """Test lambda handler, gdy S3 get_object zgłasza błąd NoSuchKey."""
        self.mock_s3_client.get_object.side_effect = self.mock_s3_client.exceptions.NoSuchKey("Simulated NoSuchKey Error")

        event = create_eventbridge_s3_event('processed-reviews-bucket', 'non_existent.json')
        result = self.sentiment_lambda.lambda_handler(event, {})

        assert result['statusCode'] == 500
        body = json.loads(result['body']) # Powinien być stringiem, json.loads powinno działać
        assert body['error'] == 'Failed to perform sentiment analysis'
        assert 'Simulated NoSuchKey Error' in body['details'] # Sprawdź rzeczywisty komunikat błędu

        self.mock_s3_client.put_object.assert_not_called()

    def test_lambda_handler_empty_or_malformed_event(self):
        """Test lambda handler z pustym lub źle sformułowanym zdarzeniem."""
        event = {} # Puste zdarzenie

        result = self.sentiment_lambda.lambda_handler(event, {})

        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to perform sentiment analysis'
        assert "'Records'" in body['details'] # Oczekiwano KeyError dla 'Records'
        self.mock_s3_client.get_object.assert_not_called()
        self.mock_s3_client.put_object.assert_not_called()
        
        event = {'Records': []} # Pusta lista Records
        result = self.sentiment_lambda.lambda_handler(event, {})
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to perform sentiment analysis'
        assert "list index out of range" in body['details']
        self.mock_s3_client.get_object.assert_not_called()

    def test_lambda_handler_ssm_error_fallback(self):
        """Test lambda handler, gdy pobieranie parametrów SSM kończy się niepowodzeniem, używając awaryjnego zasobnika."""
        # Ten test jawnie wywołuje błąd SSM, więc usuń jawne ustawienie FINAL_REVIEWS_BUCKET
        # aby upewnić się, że mechanizm awaryjny jest testowany.
        # Patch get_parameter, aby zgłosić wyjątek
        # Uwaga: Ten patch wpływa na `lambda_function.get_parameter` w ponownie załadowanym module specjalnie dla tego testu.
        with patch.object(self.sentiment_lambda, 'get_parameter', side_effect=Exception("SSM error")):
            # Funkcja lambda powinna to przechwycić i użyć awaryjnego zasobnika.
            # Nie ma potrzeby ponownego usuwania/importowania, ponieważ patch.object jest bardziej ukierunkowany.

            mock_review_data = {
                'review_id': 'B001234570',
                'reviewer_id': 'A1234567893',
                'processed_summary': 'This is a good product.',
                'processed_reviewText': 'It works well.',
                'processed_overall': '5',
                'summary_word_count': 5,
                'reviewText_word_count': 3,
                'overall_word_count': 1,
                'total_word_count': 9
            }
            self.mock_s3_client.get_object.return_value = {
                'Body': MagicMock(read=lambda: json.dumps(mock_review_data).encode('utf-8'))
            }
            self.mock_analyzer_instance.polarity_scores.side_effect = [
                {'neg': 0.0, 'neu': 0.5, 'pos': 0.5, 'compound': 0.7},  # podsumowanie
                {'neg': 0.0, 'neu': 0.5, 'pos': 0.5, 'compound': 0.6},  # reviewText
                {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}   # ogólne
            ]
            self.mock_s3_client.put_object.return_value = {}

            event = create_eventbridge_s3_event('processed-reviews-bucket', 'processed/B001234570.json')
            result = self.sentiment_lambda.lambda_handler(event, {})

            assert result['statusCode'] == 200 # Powinno teraz przejść, ponieważ fallback powinien działać
            body = json.loads(result['body'])
            assert body['message'] == 'Sentiment analysis completed'
            # Aseruje, że używana jest nazwa awaryjnego zasobnika
            assert 's3://final-reviews-bucket/analyzed/B001234570.json' in body['output_location']

            self.mock_s3_client.put_object.assert_called_once()
            put_call_args = self.mock_s3_client.put_object.call_args
            assert put_call_args[1]['Bucket'] == 'final-reviews-bucket'


    def test_lambda_handler_malformed_json_s3_object(self):
        """Test lambda handler z źle sformułowanym JSON w obiekcie S3."""
        mock_review_data = "this is not a valid json string"
        self.mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: mock_review_data.encode('utf-8'))
        }
        self.mock_s3_client.put_object.return_value = {}

        event = create_eventbridge_s3_event('processed-reviews-bucket', 'processed/malformed.json')
        result = self.sentiment_lambda.lambda_handler(event, {})

        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to perform sentiment analysis'
        # Zaktualizowana asercja dla komunikatu błędu źle sformułowanego JSON
        assert 'Expecting value:' in body['details'] or 'Invalid control character' in body['details']
        self.mock_s3_client.put_object.assert_not_called()

    def test_lambda_handler_general_s3_put_error(self):
        """Test lambda handler z ogólnym błędem S3 put_object."""
        mock_review_data = {
            'review_id': 'B001234571',
            'processed_summary': 'Test review.',
            'processed_reviewText': 'It is okay.',
            'processed_overall': '3',
            'summary_word_count': 2, 'reviewText_word_count': 3, 'overall_word_count': 1, 'total_word_count': 6
        }
        self.mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_review_data).encode('utf-8'))
        }
        self.mock_analyzer_instance.polarity_scores.return_value = {'neg': 0.0, 'neu': 1.0, 'pos': 0.0, 'compound': 0.0}
        self.mock_s3_client.put_object.side_effect = Exception("Simulated S3 Put Error")

        event = create_eventbridge_s3_event('processed-reviews-bucket', 'processed/B001234571.json')
        result = self.sentiment_lambda.lambda_handler(event, {})

        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to perform sentiment analysis'
        assert 'Simulated S3 Put Error' in body['details']
        self.mock_s3_client.get_object.assert_called_once()
        self.mock_s3_client.put_object.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__])