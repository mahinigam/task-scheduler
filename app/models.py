from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from enum import Enum
from uuid import UUID
from datetime import datetime

class Priority(str, Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"

class TaskStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TaskCreate(BaseModel):
    payload: Dict[str, Any]
    priority: Priority = Priority.MEDIUM
    execution_time: float = Field(..., description="Estimated execution time in seconds")

class TaskResponse(BaseModel):
    id: UUID
    payload: Dict[str, Any]
    priority: Priority
    status: TaskStatus
    created_at: datetime

    class Config:
        from_attributes = True
