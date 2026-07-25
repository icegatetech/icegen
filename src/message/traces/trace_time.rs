//! Time allocator for the span tree: recursively lays out each node's direct children
//! sequentially within its window so that the parent fully encloses the whole subtree.

use crate::error::GeneratorError;

/// Resolve a relative event position (`offset_frac` of the span window) to an absolute
/// timestamp, clamped into `[start_ns, end_ns]` to absorb rounding drift.
///
/// # Parameters
///
/// * `start_ns` / `end_ns` — the span window assigned by [`layout_span_tree`].
/// * `offset_frac` — position inside the window, `0.0` (start) to `1.0` (end).
pub fn resolve_event_time(start_ns: i64, end_ns: i64, offset_frac: f64) -> i64 {
    let span = (end_ns - start_ns).max(0) as f64;
    let time_ns = start_ns + (span * offset_frac).round() as i64;
    time_ns.clamp(start_ns, end_ns)
}

/// Lay out the span tree over time. Returns `(start_ns, end_ns)` windows by node index.
///
/// A node's direct children run sequentially with a `gap_ns` gap, inside the parent's window;
/// the parent window stretches to the end of the last child if the subtree is longer than its
/// own duration. When `parallel[idx]` is set, that node's direct children instead all start from
/// the same cursor, so their windows overlap (modelling parallel tool calls); the parent still
/// stretches to the latest child end. For any node `i` with parent `p` it is guaranteed that
/// `windows[p].0 <= windows[i].0 && windows[i].1 <= windows[p].1` — at any tree depth.
///
/// # Parameters
///
/// * `root_start_ns` — root start (ns).
/// * `parents` — parent index per node (`None` is the root); exactly one root is expected.
/// * `durations_ns` — nodes' own durations. Negative values are clamped to 0.
/// * `gap_ns` — gap between adjacent children (negative is treated as 0).
/// * `parallel` — per-node flag: when set, the node's direct children overlap instead of running
///   sequentially. Must be the same length as `parents`.
///
/// # Errors
///
/// Returns [`GeneratorError::MalformedSpanTree`] if the inputs do not describe a valid rooted
/// tree: mismatched slice lengths, a parent index out of bounds, zero or multiple roots, or a
/// cycle / disconnected node. Validating up front turns what would otherwise be a panic or
/// unbounded recursion in `layout_node` into a typed error.
#[allow(clippy::result_large_err)]
pub fn layout_span_tree(
    root_start_ns: i64,
    parents: &[Option<usize>],
    durations_ns: &[i64],
    gap_ns: i64,
    parallel: &[bool],
) -> Result<Vec<(i64, i64)>, GeneratorError> {
    let n = parents.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    if durations_ns.len() != n {
        return Err(GeneratorError::MalformedSpanTree(format!(
            "durations_ns length {} != parents length {n}",
            durations_ns.len()
        )));
    }
    if parallel.len() != n {
        return Err(GeneratorError::MalformedSpanTree(format!(
            "parallel length {} != parents length {n}",
            parallel.len()
        )));
    }

    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut root = None;
    for (i, parent) in parents.iter().enumerate() {
        match parent {
            Some(p) => {
                let p = *p;
                if p >= n {
                    return Err(GeneratorError::MalformedSpanTree(format!(
                        "node {i} has parent index {p} out of bounds (n={n})"
                    )));
                }
                children[p].push(i);
            }
            None => {
                if root.is_some() {
                    return Err(GeneratorError::MalformedSpanTree(
                        "more than one root (multiple None parents)".to_string(),
                    ));
                }
                root = Some(i);
            }
        }
    }
    let root = root
        .ok_or_else(|| GeneratorError::MalformedSpanTree("no root (no None parent)".to_string()))?;

    // Reachability from the root catches cycles and disconnected nodes. Done iteratively so a
    // cyclic graph cannot overflow the stack here the way recursive `layout_node` would.
    let mut visited = vec![false; n];
    let mut stack = vec![root];
    visited[root] = true;
    let mut reached = 0_usize;
    while let Some(node) = stack.pop() {
        reached += 1;
        for &child in &children[node] {
            if !visited[child] {
                visited[child] = true;
                stack.push(child);
            }
        }
    }
    if reached != n {
        return Err(GeneratorError::MalformedSpanTree(format!(
            "cycle or disconnected node: {reached}/{n} nodes reachable from the root"
        )));
    }

    let mut windows = vec![(0_i64, 0_i64); n];
    layout_node(
        root,
        root_start_ns,
        &children,
        durations_ns,
        gap_ns,
        parallel,
        &mut windows,
    );
    Ok(windows)
}

/// Lay out node `idx` from `start_ns`, recursively laying out its children. Returns the node's window end.
fn layout_node(
    idx: usize,
    start_ns: i64,
    children: &[Vec<usize>],
    durations_ns: &[i64],
    gap_ns: i64,
    parallel: &[bool],
    windows: &mut [(i64, i64)],
) -> i64 {
    let overlap = parallel[idx];
    let mut cursor = start_ns;
    let mut children_end = start_ns;
    for (i, &child) in children[idx].iter().enumerate() {
        let child_start = if overlap {
            // Parallel children share the parent's start; their ends diverge by duration.
            start_ns
        } else {
            if i > 0 {
                cursor += gap_ns.max(0);
            }
            cursor
        };
        let child_end = layout_node(
            child,
            child_start,
            children,
            durations_ns,
            gap_ns,
            parallel,
            windows,
        );
        cursor = child_end;
        children_end = children_end.max(child_end);
    }
    // the parent encloses its own duration and the entire tail of its children
    let end_ns = (start_ns + durations_ns[idx].max(0)).max(children_end);
    windows[idx] = (start_ns, end_ns);
    end_ns
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All-sequential flags for an `n`-node tree.
    fn no_parallel(n: usize) -> Vec<bool> {
        vec![false; n]
    }

    #[test]
    fn siblings_enclosed_and_ordered() {
        // root (own duration 0) + 3 children at the same level
        let parents = [None, Some(0), Some(0), Some(0)];
        let durations = [0, 100, 200, 50];
        let windows = layout_span_tree(1_000, &parents, &durations, 10, &no_parallel(4)).unwrap();
        assert_eq!(windows.len(), 4);
        // children sequential, with a gap of 10
        assert_eq!(windows[1], (1_000, 1_100));
        assert_eq!(windows[2], (1_110, 1_310));
        assert_eq!(windows[3], (1_320, 1_370));
        // root encloses all children
        let (root_start, root_end) = windows[0];
        for (s, e) in &windows[1..] {
            assert!(*s >= root_start && *e <= root_end, "child within root");
            assert!(e >= s, "end >= start");
        }
    }

    #[test]
    fn no_children_returns_own_window() {
        let windows = layout_span_tree(500, &[None], &[300], 10, &no_parallel(1)).unwrap();
        assert_eq!(windows, vec![(500, 800)]);
    }

    #[test]
    fn nested_child_stays_within_parent() {
        // root(0) → chat(1) → tool(2); tool is longer than chat — chat's window must stretch
        let parents = [None, Some(0), Some(1)];
        let durations = [0, 100, 200];
        let windows = layout_span_tree(0, &parents, &durations, 10, &no_parallel(3)).unwrap();
        let (root_s, root_e) = windows[0];
        let (chat_s, chat_e) = windows[1];
        let (tool_s, tool_e) = windows[2];
        // tool is nested within chat (not only within root)
        assert!(chat_s <= tool_s && tool_e <= chat_e, "tool within chat");
        // chat is nested within root
        assert!(root_s <= chat_s && chat_e <= root_e, "chat within root");
    }

    #[test]
    fn deeper_subtree_extends_ancestors_and_pushes_siblings() {
        // root(0) → [chat(1) → tool(2), embeddings(3)]
        // chat itself is short, but tool is long — embeddings must not overlap chat's tail
        let parents = [None, Some(0), Some(1), Some(0)];
        let durations = [0, 50, 500, 80];
        let windows = layout_span_tree(0, &parents, &durations, 10, &no_parallel(4)).unwrap();
        let (_, chat_e) = windows[1];
        let (tool_s, tool_e) = windows[2];
        let (embed_s, _) = windows[3];
        // chat is stretched to the end of tool
        assert!(tool_e <= chat_e, "tool within chat");
        // embeddings (sibling of chat) starts after chat's full window + gap
        assert!(embed_s >= chat_e, "sibling does not overlap extended chat");
        assert!(tool_s <= tool_e);
    }

    fn assert_malformed(result: Result<Vec<(i64, i64)>, GeneratorError>) {
        assert!(
            matches!(result, Err(GeneratorError::MalformedSpanTree(_))),
            "expected MalformedSpanTree, got {result:?}"
        );
    }

    #[test]
    fn empty_tree_is_ok_and_empty() {
        let windows = layout_span_tree(0, &[], &[], 10, &[]).unwrap();
        assert!(windows.is_empty());
    }

    #[test]
    fn parent_index_out_of_bounds_is_rejected() {
        let parents = [None, Some(5)];
        assert_malformed(layout_span_tree(0, &parents, &[0, 0], 10, &no_parallel(2)));
    }

    #[test]
    fn mismatched_slice_lengths_are_rejected() {
        let parents = [None, Some(0)];
        // durations too short
        assert_malformed(layout_span_tree(0, &parents, &[0], 10, &no_parallel(2)));
        // parallel too short
        assert_malformed(layout_span_tree(0, &parents, &[0, 0], 10, &no_parallel(1)));
    }

    #[test]
    fn missing_root_is_rejected() {
        // Two nodes pointing at each other: no None parent and a cycle.
        let parents = [Some(1), Some(0)];
        assert_malformed(layout_span_tree(0, &parents, &[0, 0], 10, &no_parallel(2)));
    }

    #[test]
    fn multiple_roots_are_rejected() {
        let parents = [None, None];
        assert_malformed(layout_span_tree(0, &parents, &[0, 0], 10, &no_parallel(2)));
    }

    #[test]
    fn cycle_with_valid_root_is_rejected() {
        // root 0 is fine, but 1 and 2 form a cycle unreachable from the root.
        let parents = [None, Some(2), Some(1)];
        assert_malformed(layout_span_tree(
            0,
            &parents,
            &[0, 0, 0],
            10,
            &no_parallel(3),
        ));
    }

    #[test]
    fn event_time_resolves_and_clamps_within_window() {
        // midpoint, start, end
        assert_eq!(resolve_event_time(1_000, 2_000, 0.5), 1_500);
        assert_eq!(resolve_event_time(1_000, 2_000, 0.0), 1_000);
        assert_eq!(resolve_event_time(1_000, 2_000, 1.0), 2_000);
        // out-of-range fractions are clamped into the window
        assert_eq!(resolve_event_time(1_000, 2_000, 1.5), 2_000);
        assert_eq!(resolve_event_time(1_000, 2_000, -0.5), 1_000);
        // degenerate window collapses to its single point
        assert_eq!(resolve_event_time(1_000, 1_000, 0.7), 1_000);
    }

    #[test]
    fn parallel_children_overlap_and_parent_encloses_them() {
        // root(0) with two parallel children of different durations.
        let parents = [None, Some(0), Some(0)];
        let durations = [0, 100, 250];
        let parallel = [true, false, false]; // root fans its children out in parallel
        let windows = layout_span_tree(1_000, &parents, &durations, 10, &parallel).unwrap();
        let (root_s, root_e) = windows[0];
        let (a_s, a_e) = windows[1];
        let (b_s, b_e) = windows[2];
        // both children start together (shared cursor), so their windows overlap
        assert_eq!(a_s, b_s, "parallel children share the start");
        assert!(a_s < b_e && b_s < a_e, "windows overlap");
        // parent encloses both, stretching to the later end
        assert_eq!(root_e, a_e.max(b_e));
        assert!(root_s <= a_s && a_e <= root_e && b_e <= root_e);
    }
}
