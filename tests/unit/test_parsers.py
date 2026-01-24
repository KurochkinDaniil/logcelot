"""Unit tests for log parsers (Syslog RFC5424 and CLF).

Tests parsing logic, error handling, and conversion to LogEntry format.
"""

import pytest
from datetime import datetime

from src.parsers.base import ParserError
from src.parsers.syslog import SyslogParser
from src.parsers.clf import CLFParser
from src.parsers.manager import ParserManager, parse_log
from src.models.log_entry import LogEntry


class TestSyslogParser:
    """Tests for RFC5424 Syslog parser."""

    @pytest.fixture
    def parser(self):
        """Create syslog parser instance."""
        return SyslogParser()

    def test_parse_valid_syslog(self, parser):
        """Test parsing valid RFC5424 syslog message."""
        content = "<134>1 2024-01-15T10:30:00.123Z web-server nginx 1234 ID47 - GET /api/users"

        result = parser.parse(content)

        assert result["source"] == "syslog"
        assert result["source_host"] == "web-server"
        assert result["source_service"] == "nginx"
        assert result["level"] == "INFO"  # priority 134 = facility 16, severity 6 (INFO)
        assert result["message"] == "GET /api/users"
        assert result["format"] == "syslog"
        assert result["is_parsed"] is True

        # Check metadata
        assert result["metadata"]["syslog_priority"] == 134
        assert result["metadata"]["syslog_severity"] == 6
        assert result["metadata"]["syslog_facility"] == 16
        assert result["metadata"]["proc_id"] == "1234"
        assert result["metadata"]["msg_id"] == "ID47"
        assert result["metadata"]["structured_data"] is None

    def test_parse_syslog_with_nil_values(self, parser):
        """Test parsing syslog with nil values (-)."""
        content = "<165>1 2024-01-15T10:30:00Z - app - - - Error message"

        result = parser.parse(content)

        assert result["source_host"] is None
        assert result["source_service"] == "app"
        assert result["message"] == "Error message"
        assert result["level"] == "INFO"  # severity 5 (notice) -> INFO
        assert result["metadata"]["structured_data"] is None

    def test_parse_syslog_priority_to_level_mapping(self, parser):
        """Test priority to log level conversion."""
        test_cases = [
            ("<8>1 2024-01-15T10:30:00Z host app - - - Msg", "FATAL"),   # severity 0 (emergency)
            ("<11>1 2024-01-15T10:30:00Z host app - - - Msg", "ERROR"),  # severity 3 (error)
            ("<12>1 2024-01-15T10:30:00Z host app - - - Msg", "WARN"),   # severity 4 (warning)
            ("<14>1 2024-01-15T10:30:00Z host app - - - Msg", "INFO"),   # severity 6 (info)
            ("<15>1 2024-01-15T10:30:00Z host app - - - Msg", "DEBUG"),  # severity 7 (debug)
        ]

        for content, expected_level in test_cases:
            result = parser.parse(content)
            assert result["level"] == expected_level, f"Failed for {content}"

    def test_parse_syslog_invalid_format(self, parser):
        """Test that invalid syslog raises ParserError."""
        invalid_content = "This is not a syslog message"

        with pytest.raises(ParserError) as exc_info:
            parser.parse(invalid_content)

        assert "does not match RFC5424" in str(exc_info.value)
        assert exc_info.value.format_type == "syslog"

    def test_parse_syslog_empty_content(self, parser):
        """Test that empty content raises ParserError."""
        with pytest.raises(ParserError):
            parser.parse("")

        with pytest.raises(ParserError):
            parser.parse(None)

    def test_parse_syslog_timestamp_formats(self, parser):
        """Test various timestamp formats."""
        test_cases = [
            "<134>1 2024-01-15T10:30:00Z host app - - - Msg",
            "<134>1 2024-01-15T10:30:00.123Z host app - - - Msg",
            "<134>1 2024-01-15T10:30:00+00:00 host app - - - Msg",
        ]

        for content in test_cases:
            result = parser.parse(content)
            assert "created_at" in result
            assert result["created_at"] is not None
            # created_at should be a datetime object
            from datetime import datetime
            assert isinstance(result["created_at"], datetime)

    def test_canary_syslog_examples(self, parser):
        """
        Real world-ish examples.

        Canary publishes RFC5424 syslog example entries.
        Source: https://docs.canary.tools/syslog/rfc5424.html#example-syslog-entries
        """
        canary_examples = [
            # Canarytokens Incidents / HTTP (copied from Canary docs)
            '<130>1 2025-04-30T12:09:54.681299+00:00 mycompany-com ThinkstCanary 3545385 newincident '
            '[BasicIncidentDetails@51136 Description="Web Bug Canarytoken triggered" Timestamp="2025-04-30 12:07:53 (UTC)" Reminder="q" Token="d7a7phdpurh2vs8gs1jbniyhb" SourceIP="192.168.1.97" IncidentHash="40a96cf3ba4596a81f18990143916b3c" eventid="17000"] '
            '[AdditionalIncidentDetails@51136 Abbr="SAST" Accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7" Accept-Encoding="gzip, deflate" Accept-Language="en-GB,en-US;q=0.9,en;q=0.8,ro;q=0.7" BackgroundContext="You have had 214 incidents from 192.168.1.97 previously." Browser="Chrome" Cache-Control="max-age=0" City="Cape Town" Connection="keep-alive" ContinentCode="AF" Country="South Africa" CountryCode="ZA" CountryCode3="ZAF" CurrencyCode="ZAR" Date="2025-04-30" DstPort="80" Enabled="1" Host="123456789abe[.\\]o3n[.\\]io" HostDomain="" Hostname="" Id="Africa/Johannesburg" Installed="1" Ip="192.168.1.97" IsBogon="False" IsProxy="False" IsTor="False" IsV4Mapped="False" IsV6="False" IsVpn="False" Language="en-GB" LanguageCode="zu" Latitude="-33.925552" Longitude="18.422857" Mimetypes="Portable Document Format;pdf;application/pdf|||Portable Document Format;pdf;text/pdf|||" Name="South Africa Standard Time" Offset="+02:00" Os="Macintosh" Platform="MacIntel" Region="Western Cape" RegionCode="WC" SrcPort="54290" Time="14:07:53.846452" Upgrade-Insecure-Requests="1" User-Agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36" Valid="True" Vendor="Google Inc." Version="135.0.0.0"] '
            "A Web Bug Canarytoken was triggered by '192.168.1.97'.",

            # Canarytokens Incidents / DNS
            '<130>1 2025-04-30T12:54:52.796337+00:00 mycompany-com ThinkstCanary 3557764 newincident '
            '[BasicIncidentDetails@51136 Description="DNS Canarytoken triggered" Timestamp="2025-04-30 12:52:49 (UTC)" Reminder="q" Token="vv4x12n26ivmcgyd33pkb3drr" SourceIP="1.1.1.1" IncidentHash="adaa8486af78cc450417b027e2821a22" eventid="16000"] '
            '[AdditionalIncidentDetails@51136 BackgroundContext="This alert is the first from 1.1.1.1." DstPort="53" Hostname="VV4x12N26IvMcgyd33pKB3DRr[.\\]123456789abe[.\\]o3N[.\\]Io" SrcPort="48908"] '
            "A DNS Canarytoken was triggered by a DNS query from the source IP 1.1.1.1. Please note that the source IP refers to a DNS resolver, rather than the host that triggered the token.",

            # Canarytokens Incidents / Wireguard
            '<130>1 2025-04-30T12:56:11.569505+00:00 mycompany-com ThinkstCanary 3557764 newincident '
            '[BasicIncidentDetails@51136 Description="WireGuard VPN Canarytoken triggered" Timestamp="2025-04-30 12:54:09 (UTC)" Reminder="x" Token="zcowdkas4t07sedssypwiy8pi" SourceIP="192.168.1.97" IncidentHash="e2888f4ae944ef9eb11f716a67602d01" eventid="17022"] '
            '[AdditionalIncidentDetails@51136 BackgroundContext="You have had 216 incidents from 192.168.1.97 previously." ClientPublicKey="xEUY2sgyvP/+3SaBY5OAS679m/LhMVQ3Ey8xDHBFKAc=" ClientSessionIndex="3154879465" DstPort="51820" SrcPort="57505"] '
            "A WireGuard VPN Canarytoken was triggered by '192.168.1.97'.",
        ]

        for content in canary_examples:
            result = parser.parse(content)
            assert result["is_parsed"] is True
            assert result["source"] == "syslog"
            assert result["source_service"] == "ThinkstCanary"
            assert result["metadata"]["msg_id"] == "newincident"
            assert result["metadata"]["structured_data"] is not None
            assert "BasicIncidentDetails@51136" in result["metadata"]["structured_data"]
            assert "AdditionalIncidentDetails@51136" in result["metadata"]["structured_data"]
            assert result["message"]  # should be non-empty

    def test_rfc5424_timequality(self, parser):
        """
        RFC5424 timeQuality SD element example embedded into a full message.
        """
        content = (
            '<165>1 2024-12-30T14:55:00Z host app 1234 ID47 '
            '[timeQuality tzKnown="0" isSynced="0"] '
            "Clock is not trusted"
        )

        result = parser.parse(content)
        assert result["is_parsed"] is True
        assert result["source_host"] == "host"
        assert result["source_service"] == "app"
        assert result["metadata"]["structured_data"] == '[timeQuality tzKnown="0" isSynced="0"]'
        assert result["message"] == "Clock is not trusted"

    def test_rfc5424_sd_only_message(self, parser):
        """
        RFC5424 allows SD-only messages (MSG is optional).
        """
        content = (
            '<165>1 2024-12-30T14:55:00Z host app 1234 ID47 '
            '[timeQuality tzKnown="1" isSynced="1"]'
        )

        result = parser.parse(content)
        assert result["is_parsed"] is True
        assert result["metadata"]["structured_data"] == '[timeQuality tzKnown="1" isSynced="1"]'
        assert result["message"] == ""  # no MSG part

    def test_rfc5424_multiple_sd_elements(self, parser):
        """
        RFC5424 examples: multiple SD-ELEMENTs without spaces: ...][...
        """
        content = (
            '<165>1 2024-12-30T14:55:00Z host app 1234 ID47 '
            '[exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"] '
            "Event happened"
        )

        result = parser.parse(content)
        assert result["is_parsed"] is True
        assert result["message"] == "Event happened"
        assert result["metadata"]["structured_data"] is not None
        assert '[exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"]' in result["metadata"]["structured_data"]
        assert '[examplePriority@32473 class="high"]' in result["metadata"]["structured_data"]

    def test_rfc5424_sd_with_space_between_elements_is_noncompliant_but_common(self, parser):
        """
        RFC5424 notes that ']SP[' ends structured-data, but many producers emit a space.
        Our parser is tolerant and treats it as still structured-data.
        """
        content = (
            '<165>1 2024-12-30T14:55:00Z host app 1234 ID47 '
            '[exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] '
            '[examplePriority@32473 class="high"] '
            "Event happened"
        )

        result = parser.parse(content)
        assert result["is_parsed"] is True
        assert result["message"] == "Event happened"
        assert result["metadata"]["structured_data"] is not None
        assert '[exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"]' in result["metadata"]["structured_data"]
        assert '[examplePriority@32473 class="high"]' in result["metadata"]["structured_data"]


class TestCLFParser:
    """Tests for Common Log Format (CLF) parser."""

    @pytest.fixture
    def parser(self):
        """Create CLF parser instance."""
        return CLFParser()

    def test_parse_valid_clf(self, parser):
        """Test parsing valid CLF line."""
        content = '192.168.1.1 - user123 [15/Jan/2024:10:30:00 +0000] "GET /api/users HTTP/1.1" 200 1234'

        result = parser.parse(content)

        assert result["source"] == "http"
        assert result["source_host"] == "192.168.1.1"
        assert result["source_service"] == "access-log"
        assert result["level"] == "INFO"
        assert result["format"] == "clf"
        assert result["user_id"] == "user123"
        assert result["http_method"] == "GET"
        assert result["http_path"] == "/api/users"
        assert result["http_status"] == 200
        assert result["metadata"]["body_bytes_sent"] == 1234
        assert "HTTP 200 GET /api/users" in result["message"]
        assert result["is_parsed"] is True

    def test_parse_clf_with_nil_user(self, parser):
        """Test parsing CLF with nil user (-)."""
        content = '10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "POST /api/login HTTP/1.1" 401 0'

        result = parser.parse(content)

        assert result["user_id"] is None
        assert result["http_method"] == "POST"
        assert result["http_path"] == "/api/login"
        assert result["http_status"] == 401
        assert result["level"] == "WARN"

    def test_parse_clf_status_to_level_mapping(self, parser):
        """Test HTTP status to log level conversion."""
        test_cases = [
            ('10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET /api HTTP/1.1" 200 100', "INFO"),   # 2xx
            ('10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET /api HTTP/1.1" 301 100', "INFO"),   # 3xx
            ('10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET /api HTTP/1.1" 404 100', "WARN"),   # 4xx
            ('10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET /api HTTP/1.1" 500 100', "ERROR"),  # 5xx
        ]

        for content, expected_level in test_cases:
            result = parser.parse(content)
            assert result["level"] == expected_level, f"Failed for status {result['http_status']}"

    def test_parse_combined_log_format(self, parser):
        """Test parsing Combined Log Format (with referer and user-agent)."""
        content = (
            '192.168.1.1 - user123 [15/Jan/2024:10:30:00 +0000] '
            '"GET /api/users HTTP/1.1" 200 1234 '
            '"https://example.com/home" "Mozilla/5.0"'
        )

        result = parser.parse(content)

        assert result["metadata"]["http_referer"] == "https://example.com/home"
        assert result["metadata"]["http_user_agent"] == "Mozilla/5.0"

    def test_combined_with_dash_fields(self, parser):
        """Combined logs often use '-' for empty referer."""
        content = (
            '127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] '
            '"GET /apache_pb.gif HTTP/1.0" 200 2326 '
            '"-" "Mozilla/4.08 [en] (Win98; I ;Nav)"'
        )

        result = parser.parse(content)

        assert result["http_status"] == 200
        assert result["metadata"]["body_bytes_sent"] == 2326
        assert "http_referer" not in result["metadata"]  # '-' normalized to None
        assert result["metadata"]["http_user_agent"].startswith("Mozilla/4.08")

    def test_request_can_be_dash(self, parser):
        """Some servers emit '-' instead of request line."""
        content = '10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "-" 400 -'

        result = parser.parse(content)

        assert result["http_status"] == 400
        assert result["metadata"]["body_bytes_sent"] is None
        assert result["http_method"] is None
        assert result["http_path"] is None
        assert "no-request" in result["message"]

    def test_parse_clf_invalid_format(self, parser):
        """Test that invalid CLF raises ParserError."""
        invalid_content = "This is not a CLF log"

        with pytest.raises(ParserError) as exc_info:
            parser.parse(invalid_content)

        assert "does not match Common/Combined Log Format" in str(exc_info.value)
        assert exc_info.value.format_type == "clf"

    def test_parse_clf_trailing_garbage_rejected(self, parser):
        """Ensure we don't accept valid prefix with junk at end."""
        invalid_content = '10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET / HTTP/1.1" 200 100 trailing'

        with pytest.raises(ParserError):
            parser.parse(invalid_content)

    def test_parse_clf_empty_content(self, parser):
        """Test that empty content raises ParserError."""
        with pytest.raises(ParserError):
            parser.parse("")

    def test_parse_clf_timestamp(self, parser):
        """Test CLF timestamp parsing."""
        content = '10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET / HTTP/1.1" 200 100'

        result = parser.parse(content)

        assert "created_at" in result
        assert result["created_at"] is not None
        # created_at should be a datetime object
        from datetime import datetime
        assert isinstance(result["created_at"], datetime)
        assert result["created_at"].year == 2024

    def test_parse_clf_timestamp_invalid_sets_none(self, parser):
        """Invalid timestamp should not be replaced with now()."""
        content = '10.0.0.1 - - [BAD_TIMESTAMP] "GET / HTTP/1.1" 200 100'

        result = parser.parse(content)

        assert result["created_at"] is None
        assert result["is_parsed"] is True  # tolerant behavior

    def test_parse_clf_bytes_sent(self, parser):
        """Test parsing bytes_sent field."""
        content1 = '10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET / HTTP/1.1" 200 1234'
        result1 = parser.parse(content1)
        assert result1["metadata"]["body_bytes_sent"] == 1234

        content2 = '10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET / HTTP/1.1" 304 -'
        result2 = parser.parse(content2)
        assert result2["metadata"]["body_bytes_sent"] is None


class TestParserManager:
    """Tests for ParserManager factory."""

    @pytest.fixture
    def manager(self):
        """Create parser manager instance."""
        return ParserManager()

    def test_get_parser_syslog(self, manager):
        """Test getting syslog parser."""
        parser = manager.get_parser("syslog")
        assert isinstance(parser, SyslogParser)

    def test_get_parser_clf(self, manager):
        """Test getting CLF parser."""
        parser = manager.get_parser("clf")
        assert isinstance(parser, CLFParser)

    def test_get_parser_case_insensitive(self, manager):
        """Test that parser format is case-insensitive."""
        parser1 = manager.get_parser("SYSLOG")
        parser2 = manager.get_parser("SysLog")
        parser3 = manager.get_parser("syslog")

        assert isinstance(parser1, SyslogParser)
        assert isinstance(parser2, SyslogParser)
        assert isinstance(parser3, SyslogParser)

    def test_get_parser_caching(self, manager):
        """Test that parsers are cached."""
        parser1 = manager.get_parser("syslog")
        parser2 = manager.get_parser("syslog")

        # Should return same instance
        assert parser1 is parser2

    def test_get_parser_unsupported_format(self, manager):
        """Test that unsupported format raises ParserError."""
        with pytest.raises(ParserError) as exc_info:
            manager.get_parser("invalid_format")

        assert "Unsupported format" in str(exc_info.value)

    def test_parse_log_syslog(self, manager):
        """Test parsing syslog into LogEntry."""
        content = "<134>1 2024-01-15T10:30:00Z host app - - - Test message"

        log = manager.parse_log(content, "syslog")

        assert isinstance(log, LogEntry)
        assert log.level == "INFO"
        assert log.source_service == "app"
        assert log.message == "Test message"
        assert log.is_parsed is True

    def test_parse_log_clf(self, manager):
        """Test parsing CLF into LogEntry."""
        content = '10.0.0.1 - - [15/Jan/2024:10:30:00 +0000] "GET /api HTTP/1.1" 200 100'

        log = manager.parse_log(content, "clf")

        assert isinstance(log, LogEntry)
        assert log.level == "INFO"
        assert log.http_status == 200
        assert log.is_parsed is True

    def test_parse_log_with_service_override(self, manager):
        """Test that source_service can be overridden."""
        content = "<134>1 2024-01-15T10:30:00Z host app - - - Test"

        log = manager.parse_log(content, "syslog", source_service="my-service")

        assert log.source_service == "my-service"

    def test_parse_log_invalid_content(self, manager):
        """Test parsing invalid content creates unparsed LogEntry."""
        content = "This is invalid syslog"

        log = manager.parse_log(content, "syslog", source_service="test")

        assert isinstance(log, LogEntry)
        assert log.is_parsed is False
        assert log.parse_errors is not None
        assert "does not match RFC5424" in log.parse_errors
        assert log.source_service == "test"

    def test_parse_log_json_format_error(self, manager):
        """Test that JSON format raises ParserError."""
        with pytest.raises(ParserError) as exc_info:
            manager.parse_log('{"message": "test"}', "json")

        assert "JSON format should be handled directly by API" in str(exc_info.value)

    def test_list_supported_formats(self, manager):
        """Test listing supported formats."""
        formats = manager.list_supported_formats()

        assert "syslog" in formats
        assert "clf" in formats
        assert "json" in formats


class TestParserIntegration:
    """Integration tests for parsers with LogEntry model."""

    def test_syslog_to_logentry_with_extraction(self):
        """Test that syslog parsing extracts fields correctly."""
        content = "<134>1 2024-01-15T10:30:00Z host app 1234 ID47 - Test message"

        log = parse_log(content, "syslog")

        # Check that LogEntry validators work
        assert log.id is not None
        assert log.created_at is not None
        assert log.level in ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
        # Source is explicitly set by parse_log's source parameter (defaults to "http")
        assert log.source in ["http", "syslog"]

    def test_clf_to_logentry_with_http_fields(self):
        """Test that CLF parsing populates HTTP fields."""
        content = '10.0.0.1 - user123 [15/Jan/2024:10:30:00 +0000] "POST /api/login HTTP/1.1" 401 0'

        log = parse_log(content, "clf", source_service="nginx")

        # Check HTTP-specific fields
        assert log.http_method == "POST"
        assert log.http_path == "/api/login"
        assert log.http_status == 401
        assert log.user_id == "user123"
        assert log.source_service == "nginx"

    def test_parser_error_creates_dlq_entry(self):
        """Test that parser errors create DLQ-ready LogEntry."""
        invalid_content = "Not a valid log format at all!"

        log = parse_log(invalid_content, "syslog", source_service="test")

        # Should create unparsed entry for DLQ
        assert log.is_parsed is False
        assert log.parse_errors is not None
        assert log.level == "ERROR"  # Unparsed logs are errors
        assert invalid_content[:1000] in log.message

    def test_real_world_syslog_examples(self):
        """Test with real-world syslog examples."""
        examples = [
            # Standard syslog
            "<134>1 2024-12-30T14:55:00Z server01 sshd 12345 - - Failed password for user from 10.0.0.1",
            # Application log
            "<30>1 2024-12-30T14:55:00Z app-server django 8000 REQ123 - POST /api/users returned 201",
            # System log
            "<46>1 2024-12-30T14:55:00Z db-server postgres 5432 - - Connection from 192.168.1.100",
        ]

        for content in examples:
            log = parse_log(content, "syslog")
            assert log.is_parsed is True
            assert log.level in ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]

    def test_real_world_clf_examples(self):
        """Test with real-world CLF examples."""
        examples = [
            # Nginx access log
            '192.168.1.100 - - [30/Dec/2024:14:55:00 +0000] "GET /index.html HTTP/1.1" 200 1234',
            # Apache access log with user
            '10.0.0.50 - admin [30/Dec/2024:14:55:00 +0000] "POST /admin/users HTTP/1.1" 201 567',
            # Failed request
            '172.16.0.1 - - [30/Dec/2024:14:55:00 +0000] "GET /not-found HTTP/1.1" 404 -',
        ]

        for content in examples:
            log = parse_log(content, "clf")
            assert log.is_parsed is True
            assert log.http_status is not None
            assert log.http_method in ["GET", "POST", "PUT", "DELETE", "PATCH"]
