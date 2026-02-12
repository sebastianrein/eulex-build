from sqlalchemy import String, Date, Text, ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Work(Base):
    __tablename__ = "works"

    celex_id: Mapped[str] = mapped_column(String, primary_key=True)
    document_type: Mapped[str] = mapped_column(String)
    title: Mapped[str] = mapped_column(String)
    date_adopted: Mapped[Date] = mapped_column(Date)
    language: Mapped[str] = mapped_column(String)
    full_text_html: Mapped[str] = mapped_column(Text, nullable=True)

    text_units: Mapped[list["TextUnit"]] = relationship(back_populates="work", cascade="all, delete-orphan")
    relations: Mapped[list["Relation"]] = relationship(back_populates="source_work", cascade="all, delete-orphan")


class TextUnit(Base):
    __tablename__ = "text_units"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    type: Mapped[str] = mapped_column(String)
    number: Mapped[str] = mapped_column(String)
    title: Mapped[str] = mapped_column(String, nullable=True)
    text: Mapped[str] = mapped_column(Text)

    celex_id: Mapped[str] = mapped_column(ForeignKey("works.celex_id"))
    work: Mapped["Work"] = relationship(back_populates="text_units")


class Relation(Base):
    __tablename__ = "relations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    celex_source: Mapped[str] = mapped_column(ForeignKey("works.celex_id"))
    celex_target: Mapped[str] = mapped_column(String)
    relation_type: Mapped[str] = mapped_column(String)

    source_work: Mapped["Work"] = relationship(back_populates="relations")
