import io
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from cohere_compass.clients.parser import CompassParserClient
from cohere_compass.constants import DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES, DEFAULT_PROCESSING_CHUNK_SIZE
from cohere_compass.models import CompassDocument
from cohere_compass.utils import open_document_in_chunks
from cohere_compass.exceptions import CompassClientError


class TestLargeFileProcessing(unittest.TestCase):
    """Test suite for large file processing functionality."""

    def setUp(self):
        """Set up the test environment."""
        self.parser_url = "http://test-parser-url.com"
        self.client = CompassParserClient(parser_url=self.parser_url)
        
        # Create a temporary large file
        self.temp_dir = tempfile.TemporaryDirectory()
        self.large_file_path = os.path.join(self.temp_dir.name, "large_file.txt")
        
        # Create a file just over the size limit
        self.file_size = DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES + 1000
        with open(self.large_file_path, "wb") as f:
            # Fill with repeating pattern to create the file
            chunk_size = 1024 * 1024  # 1MB chunks for writing
            pattern = b"x" * chunk_size
            
            written = 0
            while written < self.file_size:
                remaining = self.file_size - written
                if remaining < chunk_size:
                    f.write(b"x" * remaining)
                    written += remaining
                else:
                    f.write(pattern)
                    written += chunk_size
    
    def tearDown(self):
        """Clean up after test."""
        self.temp_dir.cleanup()
    
    def test_open_document_in_chunks(self):
        """Test that open_document_in_chunks correctly chunks a large file."""
        chunk_size = 10 * 1024 * 1024  # 10MB chunks
        chunks = list(open_document_in_chunks(self.large_file_path, chunk_size=chunk_size))
        
        # Calculate expected number of chunks
        expected_chunks = (self.file_size + chunk_size - 1) // chunk_size
        
        # Check that we got the expected number of chunks
        self.assertEqual(len(chunks), expected_chunks)
        
        # Check that each chunk has the correct metadata
        for i, (doc, chunk_num, total_chunks) in enumerate(chunks, 1):
            self.assertEqual(chunk_num, i)
            self.assertEqual(total_chunks, expected_chunks)
            self.assertIn("compass_original_filename", doc.content)
            self.assertEqual(doc.content["compass_original_filename"], self.large_file_path)
            self.assertEqual(doc.content["compass_chunk_number"], str(i))
            self.assertEqual(doc.content["compass_total_chunks"], str(expected_chunks))
            
            # Check the chunk size (last chunk might be smaller)
            if i < expected_chunks:
                self.assertEqual(len(doc.filebytes), chunk_size)
            else:
                self.assertEqual(len(doc.filebytes), self.file_size - (i - 1) * chunk_size)
    
    @patch('cohere_compass.clients.parser.CompassParserClient._process_file_bytes')
    def test_process_file_large_file(self, mock_process_file_bytes):
        """Test that process_file correctly chunks and processes a large file."""
        # Mock the response from _process_file_bytes with a document that includes metadata
        mock_doc = CompassDocument()
        mock_process_file_bytes.return_value = [mock_doc]
        
        # Process the large file
        result = self.client.process_file(filename=self.large_file_path)
        
        # Check that _process_file_bytes was called multiple times
        expected_chunks = (self.file_size + DEFAULT_PROCESSING_CHUNK_SIZE - 1) // DEFAULT_PROCESSING_CHUNK_SIZE
        self.assertEqual(mock_process_file_bytes.call_count, expected_chunks)
        
        # Check that all chunks were processed and returned
        self.assertEqual(len(result), expected_chunks)
        
        # Verify parent_document_id consistency
        if len(result) > 0:
            # All documents should have the same parent_document_id
            parent_id = result[0].metadata.parent_document_id
            self.assertIsNotNone(parent_id, "parent_document_id should be set")
            self.assertNotEqual(parent_id, "", "parent_document_id should not be empty")
            
            # Verify all documents have the same parent_document_id
            for doc in result:
                self.assertEqual(doc.metadata.parent_document_id, parent_id,
                                "parent_document_id should be consistent across all chunks")
    
    @patch('cohere_compass.clients.parser.CompassParserClient._process_file_bytes')
    def test_process_file_large_file_with_custom_file_id(self, mock_process_file_bytes):
        """Test that process_file correctly uses provided file_id as parent_document_id."""
        # Mock the response from _process_file_bytes
        mock_doc = CompassDocument()
        mock_process_file_bytes.return_value = [mock_doc]
        
        # Process with custom file_id
        custom_file_id = "custom-file-id-123"
        result = self.client.process_file(filename=self.large_file_path, file_id=custom_file_id)
        
        # Verify parent_document_id matches provided file_id
        for doc in result:
            self.assertEqual(doc.metadata.parent_document_id, custom_file_id,
                            "parent_document_id should match the provided file_id")
    
    @patch('cohere_compass.clients.parser.CompassParserClient._process_file_bytes')
    def test_process_file_large_file_with_chunk_error(self, mock_process_file_bytes):
        """Test handling of chunk processing errors during processing of a single large file's chunks."""
        # Mock the response from _process_file_bytes with a side effect that:
        # 1. First chunk call succeeds with a CompassDocument
        # 2. Second chunk call fails with CompassClientError
        # 3. Third chunk call would succeed again (but shouldn't be reached)
        mock_process_file_bytes.side_effect = [
            [CompassDocument(content={"id": "chunk1"})],
            CompassClientError("API failed for chunk processing"),
            [CompassDocument(content={"id": "chunk3"})],
        ]
        
        # Process the large file - should raise the exception from the failing chunk
        with self.assertRaises(CompassClientError) as context:
            self.client.process_file(filename=self.large_file_path)
        
        # Verify the error message
        self.assertIn("API failed for chunk processing", str(context.exception))
        
        # Verify that _process_file_bytes was called exactly 2 times
        # (once for successful chunk, once for failing chunk)
        self.assertEqual(mock_process_file_bytes.call_count, 2)
        
        # Test through process_files to ensure error tuple is yielded properly
        mock_process_file_bytes.reset_mock()
        mock_process_file_bytes.side_effect = [
            [CompassDocument(content={"id": "chunk1"})],
            CompassClientError("API failed for chunk processing"),
            [CompassDocument(content={"id": "chunk3"})],
        ]
        
        results = list(self.client.process_files(filenames=[self.large_file_path]))
        
        # Should contain one result that is a tuple with the error
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], tuple)
        self.assertEqual(results[0][0], self.large_file_path)
        self.assertIsInstance(results[0][1], CompassClientError)
        self.assertIn("API failed for chunk processing", str(results[0][1]))
        
        # Verify mock was called the expected number of times
        self.assertEqual(mock_process_file_bytes.call_count, 2)


if __name__ == "__main__":
    unittest.main() 