from dataclasses import dataclass, field


@dataclass
class Tags:
    tags: set[str] = field(default_factory=set)
    name_add_to_tags: bool = True
    schema_add_to_tags: bool = True
    model_gets_all_tag: bool = True
    unkebab_name_for_tags: bool = True
    unkebab_schema_for_tags: bool = True

    def assemble(self, name: str, schema: str) -> set[str]:
        result = set(self.tags)
        if self.name_add_to_tags:
            result.add(name)
        if self.schema_add_to_tags:
            result.add(schema)
        if self.model_gets_all_tag:
            result.add("all")
        if self.unkebab_schema_for_tags:
            result.update(schema.split("_"))
        if self.unkebab_name_for_tags:
            result.update(name.split("_"))
        return result
