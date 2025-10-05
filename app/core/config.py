from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "Document Processing RAG System"
    PROJECT_VERSION: str = "1.0.0"

    class Config:
        case_sensitive = True


settings = Settings()