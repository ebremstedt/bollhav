# bollhav skills

Claude Code skills for building [bollhav](https://github.com/ebremstedt/bollhav)
data pipelines. Each one teaches part of the workflow — the Model API, the
recommended project layout, tag targeting, the run env vars, and an
interactive pipeline designer.

Every `SKILL.md` is plain markdown — read them right here on GitHub, or drop
the ones you want into a Claude skills directory to use them.

## Use them with Claude

Copy a skill folder into your skills directory:

```bash
# personal — available in every project
cp -r skills/guide ~/.claude/skills/

# or per-project — commit it with the repo
cp -r skills/guide /path/to/your/repo/.claude/skills/
```

Then ask Claude, e.g. *"run the bollhav guide"* or *"how do I target one model
with TAGS?"*. Claude picks the right skill from its description.

## The skills

| Skill | What it does |
|---|---|
| `overview` | What bollhav is and how the pieces fit — the map. Start here to orient. |
| `pipeline-pattern` | The recommended project layout: `src/main.py` holding the three decorators, one `Model` per file in `src/models/`, and read/transform/write in their own files. |
| `tags` | Setting tags on a model + targeting models with the `TAGS=` expression syntax (`[Name]`, `[fact]`, `[a & b]`, `[a][b]`, `not:`, `r:`). |
| `env-vars` | The command that runs models locally — windows, run modes, schema suffixes (set inline, not exported). |
| `guide` | **Interactive.** Answers a few questions about your data and proposes a concrete `Model(...)`. Best entry point for building something new. |

## Layout

```
skills/
├── overview/SKILL.md
├── pipeline-pattern/SKILL.md
├── tags/SKILL.md
├── env-vars/SKILL.md
└── guide/SKILL.md
```
