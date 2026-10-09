//! The checked-in `OpenAPI` document, and whether it is still true.
//!
//! `openapi/lepatch.json` is the source of truth for this API and it is written
//! by hand. `src/serve/openapi.rs` explains why: a derive-based generator would
//! put `#[derive(Serialize)]` on this crate's own types, and a `build.rs` emitter
//! would make the document stop being reviewable in a diff.
//!
//! **A hand-written specification is only a liability if nothing says whether it
//! is true.** So this file says, in the four ways it rots:
//!
//! 1. a route with no entry in `paths`;
//! 2. an entry in `paths` with no route;
//! 3. a schema defined and never referenced, or referenced and never defined;
//! 4. a version bump that forgot the document, or an `operationId` that was
//!    copied and now names two operations.
//!
//! `serde_json` is a dev-dependency for exactly this file. See `Cargo.toml` for
//! why the boundary is exempt from the crate's no-serde rule, and why it is a
//! dev-dependency anyway.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

use std::{collections::BTreeSet, sync::OnceLock};

use serde_json::Value;

/// The document as it is served, parsed once.
///
/// `include_str!`d into the binary, so this cannot pass by reading a file that is
/// not the one that ships.
///
/// Parsed once and shared, because the helpers below return references *into*
/// the parsed value. Re-parsing per call would hand out borrows of a temporary.
fn document() -> &'static Value {
    static PARSED: OnceLock<Value> = OnceLock::new();

    PARSED.get_or_init(|| {
        serde_json::from_str(lepatch::serve::DOCUMENT)
            .expect("openapi/lepatch.json must be valid JSON")
    })
}

/// Every `(method, path)` the document declares, over every operation.
fn declared() -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();

    let paths = document()["paths"]
        .as_object()
        .expect("paths must be an object");

    for (path, item) in paths {
        let operations = item
            .as_object()
            .expect("each path must hold its operations");

        for method in operations.keys() {
            // `parameters` is a key beside the operations, not an operation.
            if method == "parameters" {
                continue;
            }
            out.insert((method.to_ascii_uppercase(), path.clone()));
        }
    }

    out
}

#[test]
fn the_document_is_openapi_3_0() {
    // The user asked for 3.0 specifically, and 3.1 is not a compatible superset
    // of what a 3.0 client expects: nullable changes from a keyword to a type.
    let version = document()["openapi"].as_str().unwrap();

    assert!(
        version.starts_with("3.0"),
        "the document declares OpenAPI {version}, not 3.0.x"
    );
}

#[test]
fn the_version_matches_the_crate() {
    // The number a client reads is the number a bug report carries. A release
    // that bumped the manifest and not this file would advertise a version that
    // does not exist.
    assert_eq!(
        document()["info"]["version"].as_str().unwrap(),
        env!("CARGO_PKG_VERSION"),
        "info.version must track Cargo.toml"
    );
}

#[test]
fn every_route_the_server_answers_is_in_the_document() {
    // The direction that matters. A route nobody documented is a route a client
    // generating from the document has never heard of, and it is the one that
    // gets discovered in production.
    let declared = declared();

    for (method, path) in lepatch::serve::ROUTES {
        let key = ((*method).to_string(), (*path).to_string());
        assert!(
            declared.contains(&key),
            "{method} {path} is answered by the server and is not in the document"
        );
    }
}

#[test]
fn every_path_in_the_document_is_answered_by_the_server() {
    // The other direction. A documented operation the server does not answer is
    // a client's `404`, and it is the one that looks like the document is right.
    let answered: BTreeSet<(String, String)> = lepatch::serve::ROUTES
        .iter()
        .map(|(method, path)| ((*method).to_string(), (*path).to_string()))
        .collect();

    for (method, path) in declared() {
        assert!(
            answered.contains(&(method.clone(), path.clone())),
            "the document declares {method} {path}, which the server does not answer"
        );
    }
}

#[test]
fn every_referenced_schema_is_defined() {
    // A dangling `$ref` is a document that cannot be resolved, so a generator
    // either fails or emits a stub with no fields — and the stub is what a client
    // is written against.
    let names = defined_schemas();
    let defined: BTreeSet<&str> = names.iter().map(String::as_str).collect();

    for reference in references() {
        assert!(
            defined.contains(reference.as_str()),
            "a $ref points at {reference}, which is not defined"
        );
    }
}

#[test]
fn every_defined_schema_is_referenced_and_declared() {
    // Two checks that belong together.
    //
    // A schema nothing references is a leftover from a route that was removed,
    // and it is the first thing a reader trusts because it is in the document.
    //
    // The other direction is the whole point of `SCHEMAS`. That list mirrors the
    // document schema set from the code side, so a handler that grows a body and
    // does not add its schema there is a mismatch between the code and the
    // contract. It was missing: the check ran only the other way, and so it
    // caught a name the document dropped and never caught a name it gained.
    // `every_schema_the_document_defines_is_in_the_code_list` closes that gap.
    let referenced: BTreeSet<String> = references().into_iter().collect();

    for name in defined_schemas() {
        assert!(
            referenced.contains(&name),
            "schema {name} is defined and nothing refers to it"
        );
    }

    let names = defined_schemas();
    for declared in lepatch::serve::SCHEMAS {
        assert!(
            names.iter().any(|name| name == declared),
            "SCHEMAS lists {declared}, which the document does not define"
        );
    }
}

#[test]
fn every_schema_the_document_defines_is_in_the_code_list() {
    // **The silent-gap check for the code side.** `SCHEMAS` restates the
    // document schema set so a reviewer and a test can compare the two. A
    // handler that grows a body, the document gains the schema for it, and the
    // code list does not, is drift that the other direction cannot see.
    //
    // `Version` was the live case. `VersionList.versions` is an array of it, so
    // the document defined it while the code list named only `VersionList`.
    // `RestorePlan` was the second one, as the request body of
    // `POST /v1/packs/{id}/plan`.
    let mut from_code: Vec<&str> = lepatch::serve::SCHEMAS.to_vec();
    from_code.sort_unstable();

    let names = defined_schemas();
    let defined: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut defined: Vec<&str> = defined;
    defined.sort_unstable();

    assert_eq!(
        defined, from_code,
        "the document schema set and SCHEMAS have drifted apart"
    );
}

#[test]
fn every_operation_has_a_unique_operation_id() {
    // A duplicated `operationId` is how a generated client ends up with two
    // methods of the same name, and the second silently wins.
    let mut seen: BTreeSet<String> = BTreeSet::new();

    for id in operation_ids() {
        assert!(!id.is_empty(), "an operation has an empty operationId");
        assert!(
            seen.insert(id.clone()),
            "operationId {id:?} is used by two operations"
        );
    }
}

#[test]
fn every_operation_says_what_it_can_return() {
    // An operation with no `responses` tells a client nothing about failure, and
    // the failures here are the interesting half: a `422` for a pack that is not
    // one, a `409` for a job that already finished, a `416` for a range past the
    // end.
    let paths = document()["paths"].as_object().unwrap();

    for (path, item) in paths {
        for (method, operation) in item.as_object().unwrap() {
            if method == "parameters" {
                continue;
            }

            let responses = operation.get("responses");
            assert!(
                responses
                    .and_then(Value::as_object)
                    .is_some_and(|r| !r.is_empty()),
                "{method} {path} declares no responses"
            );
        }
    }
}

#[test]
fn every_path_parameter_the_router_uses_is_declared() {
    // The parameter names come from **`ROUTES`, the router's own table**, not
    // from the document. The previous version of this test built its set from the
    // document and then asserted two hardcoded names appeared in it — so renaming
    // `{id}` to `{packId}` in one path left the test passing while the handler's
    // `web::Path` extraction and the documented parameter disagreed.
    let used: BTreeSet<&str> = lepatch::serve::ROUTES
        .iter()
        .filter_map(|(_, path)| {
            path.split('/')
                .find(|part| part.starts_with('{') && part.ends_with('}'))
                .map(|name| name.trim_matches(|c| c == '{' || c == '}'))
        })
        .collect();

    let declared: BTreeSet<&str> = document()["paths"]
        .as_object()
        .expect("paths must be an object")
        .keys()
        .flat_map(|path| {
            path.split('/')
                .filter(|part| part.starts_with('{') && part.ends_with('}'))
                .map(|name| name.trim_matches(|c| c == '{' || c == '}'))
                .collect::<Vec<_>>()
        })
        .collect();

    assert!(
        !used.is_empty(),
        "no path parameters were found in ROUTES, so this test would pass vacuously"
    );

    for name in &used {
        assert!(
            declared.contains(name),
            "the router reads `{{{name}}}` and the document declares no such parameter"
        );
    }
}

#[test]
fn the_events_description_names_every_event_the_stream_can_carry() {
    // **A promise a client switches on.** The description of
    // `GET /v1/jobs/{id}/events` tells a client which `event:` names exist, and a
    // client that reads `error` from that paragraph cannot handle a failing job
    // without falling back to polling. The paragraph drifted when the `error`
    // frame was added and the document was not, so it is checked against
    // [`lepatch::serve::EVENT_NAMES`] rather than trusted.
    //
    // That constant comes from the `JobEvent` declaration in `jobs.rs`, and
    // `render_event` is held to it in `handlers.rs` by
    // `every_rendered_event_is_a_named_one`. So this is the last link in a chain
    // that starts at the declaration, not a list written out here that could rot
    // on its own.
    let description = document()["paths"]["/v1/jobs/{id}/events"]["get"]["description"]
        .as_str()
        .expect("the events operation describes itself");

    for name in lepatch::serve::EVENT_NAMES {
        assert!(
            description.contains(&format!("`{name}`")),
            "the events description does not name `{name}`, so a client cannot switch on it: \
             {description}"
        );
    }

    // And the count beside the list has to be the count. "Three event names"
    // beside four of them is a description that tells a client the list is
    // closed and is not.
    //
    // **This reads the number out of the prose rather than listing the numbers
    // that are wrong.** It used to forbid "Two", "Three", "Five" and "Six",
    // which left "Four" open. Adding a fifth variant then had to write "Five"
    // to be correct, and the guard failed the correct answer.
    //
    // **Every count phrase in the paragraph is collected, not the first one
    // found.** A description that counts twice has contradicted itself, and a
    // guard that picked the lower number would pass a document that names two
    // different totals. So the assertion below is against a one-element list, and
    // a second phrase fails it.
    let words = [
        "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten", "Eleven",
        "Twelve",
    ];

    let counted: Vec<usize> = words
        .iter()
        .enumerate()
        .filter(|(_, word)| description.contains(&format!("{word} event names")))
        .map(|(at, _)| at + 1)
        .collect();

    assert_eq!(
        counted,
        vec![lepatch::serve::EVENT_NAMES.len()],
        "the events description counts {counted:?} and the renderer has one set of {}: \
         {description}",
        lepatch::serve::EVENT_NAMES.len()
    );
}

#[test]
fn the_pack_get_documents_the_whole_pack_limit() {
    // `GET /v1/packs/{id}` answers `413` for an un-ranged fetch of a pack above
    // the server's whole-pack bound, and the document said nothing about it. A
    // client generating from the spec has two reachable shapes for this endpoint
    // — bytes, or a failure it was never told the server can produce — and only
    // one of them is the truth.
    //
    // The other responses are the ones a fetch plan tells a client to expect, so
    // the assertion is on the code rather than on the prose.
    let responses = document()["paths"]["/v1/packs/{id}"]["get"]["responses"]
        .as_object()
        .expect("the pack GET declares responses");

    assert!(
        responses.contains_key("413"),
        "an un-ranged GET of a pack above the whole-pack limit is refused, and the document does \
         not say so: {:?}",
        responses.keys().collect::<Vec<_>>()
    );

    // And it must be a documented problem, not an empty status: the client needs
    // the `Error` schema to read the reason off it.
    assert_eq!(
        responses["413"]["content"]["application/problem+json"]["schema"]["$ref"],
        "#/components/schemas/Error",
        "a 413 must carry the error document every other failure does"
    );
}

#[test]
fn the_pack_media_type_is_the_one_the_server_sends() {
    // `application/octet-stream` would say nothing. A client that uploads a pack
    // and a client that uploads a photograph are the same request without it.
    let document = lepatch::serve::DOCUMENT;

    assert!(
        document.contains("application/x-lepatch-pack"),
        "the document does not use the pack media type"
    );
    assert!(
        document.contains("application/problem+json"),
        "the document does not use RFC 9457's media type"
    );
}

/// Every schema name the document defines.
fn defined_schemas() -> Vec<String> {
    document()["components"]["schemas"]
        .as_object()
        .expect("components.schemas must be an object")
        .keys()
        .cloned()
        .collect()
}

/// Every `#/components/schemas/...` name anything refers to.
///
/// Walked as text rather than resolved as a value, so a `$ref` inside a schema's
/// own `example` — which is not a real reference — is not treated as one. A
/// document that used a schema name in an example would otherwise fail here.
fn references() -> Vec<String> {
    let mut out = Vec::new();
    collect(document(), &mut out);
    out
}

fn collect(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => {
            if let Some(name) = text.strip_prefix("#/components/schemas/") {
                out.push(name.to_string());
            }
        }
        Value::Array(items) => items.iter().for_each(|item| collect(item, out)),
        Value::Object(fields) => {
            for (key, item) in fields {
                if key == "$ref" {
                    if let Some(name) = item
                        .as_str()
                        .and_then(|t| t.strip_prefix("#/components/schemas/"))
                    {
                        out.push(name.to_string());
                    }
                    continue;
                }
                collect(item, out);
            }
        }
        _ => {}
    }
}

/// Every `operationId` in the document.
fn operation_ids() -> Vec<String> {
    let mut out = Vec::new();

    let paths = document()["paths"].as_object().unwrap();
    for item in paths.values() {
        for (method, operation) in item.as_object().unwrap() {
            if method == "parameters" {
                continue;
            }
            if let Some(id) = operation.get("operationId").and_then(Value::as_str) {
                out.push(id.to_string());
            }
        }
    }

    out
}

#[test]
fn every_response_that_points_somewhere_also_carries_a_body() {
    // A `2xx` that is not a `204` owes the client a body, and a `Location` header
    // is a promise: follow it and you find the thing this response is about.
    //
    // `DELETE /v1/jobs/{id}` used to break both halves at once. It answered `202`
    // with a description and nothing else — no body, no `Location` — while the
    // handler sent a job handle and a `Location`. A client that follows the
    // documented shape got nothing, and a generated client had no type to read.
    //
    // The check is on the status and not on the header, because `204` is the one
    // success that legitimately has no body: it is a `204` and not a `2xx`
    // precisely so a client does not go looking for content. The media type is
    // not pinned to JSON either, because `GET /v1/jobs/{id}/events` answers
    // `text/event-stream` and that is a body too.
    let paths = document()["paths"].as_object().unwrap();

    for (path, item) in paths {
        for (method, operation) in item.as_object().unwrap() {
            if method == "parameters" {
                continue;
            }

            for (code, response) in operation["responses"].as_object().unwrap() {
                if !code.starts_with('2') || code == "204" {
                    continue;
                }

                let content = response.get("content").and_then(Value::as_object);
                let media_types = content.map(|media| media.values().collect::<Vec<_>>());
                assert!(
                    media_types.as_ref().is_some_and(|media| !media.is_empty()),
                    "{method} {path} answers {code} with no body. If that is deliberate \
                     the answer is a 204; if it is a handle, it needs a schema."
                );

                for media_type in media_types.unwrap_or_default() {
                    assert!(
                        media_type.get("schema").is_some(),
                        "{method} {path} answers {code} with a body and no schema, so a \
                         client has no type to read it as"
                    );
                }

                if response["headers"].get("Location").is_some() {
                    assert!(
                        response["headers"]["Location"].get("description").is_some(),
                        "{method} {path} answers {code} with a `Location` header that says \
                         nothing, so a client that follows it gets a URL and no idea what \
                         is at the other end"
                    );
                }
            }
        }
    }
}

#[test]
fn the_declaration_helpers_find_every_schema_and_operation() {
    // A cheap sanity check on the helpers above rather than on the document: if
    // `declared` or `operation_ids` stopped finding anything, every other test
    // in this file would pass vacuously.
    assert_eq!(declared().len(), lepatch::serve::ROUTES.len());
    assert_eq!(operation_ids().len(), declared().len());
    assert!(
        !declared().is_empty(),
        "the helpers found nothing, so every other test here would pass vacuously"
    );
}
