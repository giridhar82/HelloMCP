import re
import sqlparse

from ...models import QueryRiskAssessment, RiskLevel


class SQLRiskChecker:
    def __init__(self):
        self.dangerous_patterns = {
            "drop_table": re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE),
            "drop_database": re.compile(r"\bDROP\s+DATABASE\b", re.IGNORECASE),
            "truncate_table": re.compile(r"\bTRUNCATE\s+TABLE\b", re.IGNORECASE),
            "delete_all": re.compile(r"\bDELETE\s+(FROM\s+)?\w+\s*$", re.IGNORECASE),
            "update_all": re.compile(r"\bUPDATE\s+\w+\s+SET\b.*\bWHERE\s*$", re.IGNORECASE),
            "alter_table_drop": re.compile(r"\bALTER\s+TABLE\s+\w+\s+DROP\b", re.IGNORECASE),
            "grant_all": re.compile(r"\bGRANT\s+ALL\b", re.IGNORECASE),
            "revoke_all": re.compile(r"\bREVOKE\s+ALL\b", re.IGNORECASE),
            "exec": re.compile(r"\bEXEC\b|\bEXECUTE\b", re.IGNORECASE),
            "xp_cmdshell": re.compile(r"\bxp_cmdshell\b", re.IGNORECASE),
        }
        self.risk_weights = {
            "dangerous_operation": 30,
            "data_modification": 15,
            "schema_change": 25,
            "no_where_clause": 20,
            "wildcard_select": 10,
            "system_command": 40,
            "complex_query": 5,
        }
        self.max_risk_score = 100
        self.risk_thresholds = {"low": 20, "medium": 40, "high": 70, "critical": 90}

    async def assess_query_risk(self, query: str) -> QueryRiskAssessment:
        parsed = sqlparse.parse(query)
        if not parsed:
            return QueryRiskAssessment(risk_level=RiskLevel.HIGH, risk_score=80.0, is_safe=False, recommendation="Unable to parse SQL query", warnings=["Invalid SQL syntax"])

        score = 0.0
        warnings = []
        dangerous = []
        safe = True

        for name, pattern in self.dangerous_patterns.items():
            if pattern.search(query):
                score += self.risk_weights["dangerous_operation"]
                dangerous.append(name)
                warnings.append(f"Dangerous operation detected: {name}")
                safe = False

        if self._is_data_modification_query(query):
            score += self.risk_weights["data_modification"]
            warnings.append("Data modification operation detected")

        if self._is_schema_change_query(query):
            score += self.risk_weights["schema_change"]
            warnings.append("Schema modification operation detected")

        if self._has_missing_where_clause(query):
            score += self.risk_weights["no_where_clause"]
            warnings.append("UPDATE/DELETE query without WHERE clause")
            safe = False

        if self._has_wildcard_select(query):
            score += self.risk_weights["wildcard_select"]
            warnings.append("Wildcard SELECT statement detected")

        if self._contains_system_commands(query):
            score += self.risk_weights["system_command"]
            warnings.append("System command detected")
            safe = False

        score += self._calculate_complexity_score(query) * self.risk_weights["complex_query"]
        score = min(score, self.max_risk_score)
        level = self._determine_risk_level(score)
        rec = self._recommendation(level, dangerous, warnings)

        return QueryRiskAssessment(risk_level=level, risk_score=score, is_safe=safe and score < self.risk_thresholds["high"], warnings=warnings, dangerous_operations=dangerous, recommendation=rec)

    def _is_data_modification_query(self, query: str) -> bool:
        pats = [re.compile(r"\bINSERT\s+INTO\b", re.IGNORECASE), re.compile(r"\bUPDATE\s+\w+\s+SET\b", re.IGNORECASE), re.compile(r"\bDELETE\s+FROM\b", re.IGNORECASE), re.compile(r"\bMERGE\s+INTO\b", re.IGNORECASE)]
        return any(p.search(query) for p in pats)

    def _is_schema_change_query(self, query: str) -> bool:
        pats = [re.compile(r"\bCREATE\s+TABLE\b", re.IGNORECASE), re.compile(r"\bALTER\s+TABLE\b", re.IGNORECASE), re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE), re.compile(r"\bCREATE\s+INDEX\b", re.IGNORECASE), re.compile(r"\bDROP\s+INDEX\b", re.IGNORECASE), re.compile(r"\bCREATE\s+VIEW\b", re.IGNORECASE), re.compile(r"\bDROP\s+VIEW\b", re.IGNORECASE)]
        return any(p.search(query) for p in pats)

    def _has_missing_where_clause(self, query: str) -> bool:
        up = re.compile(r"\bUPDATE\s+\w+\s+SET\b", re.IGNORECASE)
        de = re.compile(r"\bDELETE\s+(FROM\s+)?\w+\s*$", re.IGNORECASE)
        q = re.sub(r"--.*$", "", query, flags=re.MULTILINE)
        q = re.sub(r"/\*.*?\*/", "", q, flags=re.DOTALL).strip()
        if up.search(q) or de.search(q):
            return not re.search(r"\bWHERE\b.*$", q, re.IGNORECASE)
        return False

    def _has_wildcard_select(self, query: str) -> bool:
        return bool(re.search(r"\bSELECT\s+\*\b", query, re.IGNORECASE))

    def _contains_system_commands(self, query: str) -> bool:
        pats = [re.compile(r"\bSHUTDOWN\b", re.IGNORECASE), re.compile(r"\bBACKUP\s+DATABASE\b", re.IGNORECASE), re.compile(r"\bRESTORE\s+DATABASE\b", re.IGNORECASE), re.compile(r"\bEXEC\s+sp_", re.IGNORECASE), re.compile(r"\bxp_cmdshell\b", re.IGNORECASE)]
        return any(p.search(query) for p in pats)

    def _calculate_complexity_score(self, query: str) -> float:
        factors = {
            "subqueries": len(re.findall(r"\bSELECT\b.*\bFROM\b.*\(\s*SELECT\b", query, re.IGNORECASE | re.DOTALL)),
            "joins": len(re.findall(r"\b(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN)\b", query, re.IGNORECASE)),
            "unions": len(re.findall(r"\bUNION\b", query, re.IGNORECASE)),
            "group_by": len(re.findall(r"\bGROUP\s+BY\b", query, re.IGNORECASE)),
            "having": len(re.findall(r"\bHAVING\b", query, re.IGNORECASE)),
            "order_by": len(re.findall(r"\bORDER\s+BY\b", query, re.IGNORECASE)),
            "case_statements": len(re.findall(r"\bCASE\b", query, re.IGNORECASE)),
            "functions": len(re.findall(r"\b(COUNT|SUM|AVG|MIN|MAX|CONCAT|SUBSTRING)\b", query, re.IGNORECASE)),
        }
        total = sum(factors.values())
        return min(total / 10.0, 1.0)

    def _determine_risk_level(self, score: float) -> RiskLevel:
        if score >= self.risk_thresholds["critical"]:
            return RiskLevel.CRITICAL
        if score >= self.risk_thresholds["high"]:
            return RiskLevel.HIGH
        if score >= self.risk_thresholds["medium"]:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW

    def _recommendation(self, level: RiskLevel, dangerous: list[str], warnings: list[str]) -> str:
        if level == RiskLevel.CRITICAL:
            return "Query blocked: Critical risk level"
        if level == RiskLevel.HIGH:
            return "High risk query"
        if level == RiskLevel.MEDIUM:
            return "Medium risk query"
        if dangerous:
            return f"Dangerous operations: {', '.join(dangerous)}"
        if warnings:
            return warnings[0]
        return "Query appears safe"