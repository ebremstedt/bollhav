import re
from dataclasses import dataclass, field


def _split_pascal(s: str) -> list[str]:
    return [
        part.lower() for part in re.findall(r"[A-Z][a-z]*|[a-z]+|[0-9]+", s) if part
    ]


@dataclass
class Tags:
    tags: set[str] = field(default_factory=set)
    name_add_to_tags: bool = True
    schema_add_to_tags: bool = True
    catalog_add_to_tags: bool = True
    model_gets_all_tag: bool = True
    unsnake_name_for_tags: bool = True
    unsnake_schema_for_tags: bool = True
    unsnake_catalog_for_tags: bool = True
    unpascal_name_for_tags: bool = False
    unpascal_schema_for_tags: bool = False
    unpascal_catalog_for_tags: bool = False
    full_name_add_to_tags: bool = True
    fully_qualified_name_add_to_tags: bool = True
    suffix_add_to_tags: bool = True

    def assemble(self, name: str, schema: str, catalog: str | None = None) -> set[str]:
        """Build the model's tag set from explicit tags plus auto-derived
        ones (name, schema, catalog, dotted combinations, and snake/pascal
        variants). Catalog-derived tags only fire when `catalog` is set —
        a `None`/empty catalog adds nothing."""
        result = set(self.tags)
        if self.name_add_to_tags:
            result.add(name)
        if self.schema_add_to_tags:
            result.add(schema)
        if self.catalog_add_to_tags and catalog:
            result.add(catalog)
        if self.model_gets_all_tag:
            result.add("all")
        if self.full_name_add_to_tags and schema:
            result.add(f"{schema}.{name}")
        if self.fully_qualified_name_add_to_tags and catalog and schema:
            result.add(f"{catalog}.{schema}.{name}")
        if self.unsnake_schema_for_tags:
            result.update(schema.split("_"))
        if self.unsnake_name_for_tags:
            result.update(name.split("_"))
        if self.unsnake_catalog_for_tags and catalog:
            result.update(catalog.split("_"))
        if self.unpascal_name_for_tags:
            result.update(_split_pascal(name))
        if self.unpascal_schema_for_tags:
            result.update(_split_pascal(schema))
        if self.unpascal_catalog_for_tags and catalog:
            result.update(_split_pascal(catalog))
        return result
