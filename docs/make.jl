using Documenter

makedocs(
    sitename = "CirQuant",
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", "false") == "true",
        assets = ["../images/CirQuant_logo_small.png"],
        logo = "../images/CirQuant_logo_small.png",
    ),
    pages = [
        "Home" => "index.md",
        "Methodology" => "methodology.md",
        "Configuration Guide" => "configuration-guide.md",
        "Parameters Reference" => "parameters-reference.md",
        "Data Sources" => "data-sources.md",
        "Database Schema" => [
            "Raw" => "database-schema-raw.md",
            "Processed" => "database-schema-processed.md",
        ],
        "Roadmap" => "roadmap.md",
    ],
)

deploydocs(
    repo = "github.com/equicirco/cirquant.git",
    devbranch = "main",
)
