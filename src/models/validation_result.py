from dataclasses import dataclass

@dataclass
class ValidationResult:
    source_id: str
    source_type: str
    check_type: str
    check_name: str
    status: str
    remarks: str = ""

    def to_dict(self):
        """Convert to dictionary for Spark DataFrame creation"""
        return {
            "source_id": self.source_id,
            "source_type": self.source_type,
            "check_type": self.check_type,
            "check_name": self.check_name,
            "status": self.status,
            "remarks": self.remarks
        }