use std::{
    cell::RefCell,
    fmt, mem, str,
    sync::atomic::{self, AtomicUsize, Ordering},
};

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use owning_ref::OwningHandle;

use std::collections::HashSet;
pub(crate) use tracing_core::span::{Attributes, Id, Record};
use tracing_core::{dispatcher, Metadata};
use tracing_subscriber::fmt::format::{FormatFields};
use tracing_subscriber::field::RecordFields;


#[macro_use]
macro_rules! try_lock {
    ($lock:expr) => {
        try_lock!($lock, else return)
    };
    ($lock:expr, else $els:expr) => {
        match $lock {
            Ok(l) => l,
            Err(_) if std::thread::panicking() => $els,
            Err(_) => panic!("lock poisoned"),
        }
    };
}

pub struct Span<'a> {
    lock: OwningHandle<RwLockReadGuard<'a, Slab>, RwLockReadGuard<'a, Slot>>,
}

/// Represents the `Subscriber`'s view of the current span context to a
/// formatter.
#[derive(Debug)]
pub struct Context<'a, F> {
    store: &'a Store,
    fmt_fields: &'a F,
}

/// Stores data associated with currently-active spans.
#[derive(Debug)]
pub(crate) struct Store {
    // Active span data is stored in a slab of span slots. Each slot has its own
    // read-write lock to guard against concurrent modification to its data.
    // Thus, we can modify any individual slot by acquiring a read lock on the
    // slab, and using that lock to acquire a write lock on the slot we wish to
    // modify. It is only necessary to acquire the write lock here when the
    // slab itself has to be modified (i.e., to allocate more slots).
    inner: RwLock<Slab>,

    // The head of the slab's "free list".
    next: AtomicUsize,
}

#[derive(Debug)]
pub(crate) struct Data {
    parent: Option<Id>,
    metadata: &'static Metadata<'static>,
    ref_count: AtomicUsize,
    is_empty: bool,
}

#[derive(Debug)]
struct Slab {
    slab: Vec<RwLock<Slot>>,
}

#[derive(Debug)]
struct Slot {
    fields: String,
    span: State,
}

#[derive(Debug)]
enum State {
    Full(Data),
    Empty(usize),
}

struct ContextId {
    id: Id,
    duplicate: bool,
}

struct SpanStack {
    stack: Vec<ContextId>,
    ids: HashSet<Id>,
}

impl SpanStack {
    fn new() -> Self {
        SpanStack {
            stack: vec![],
            ids: HashSet::new(),
        }
    }

    fn push(&mut self, id: Id) {
        let duplicate = self.ids.contains(&id);
        if !duplicate {
            self.ids.insert(id.clone());
        }
        self.stack.push(ContextId { id, duplicate })
    }

    fn pop(&mut self, expected_id: &Id) -> Option<Id> {
        if &self.stack.last()?.id == expected_id {
            let ContextId { id, duplicate } = self.stack.pop()?;
            if !duplicate {
                self.ids.remove(&id);
            }
            Some(id)
        } else {
            None
        }
    }

    #[inline]
    fn current(&self) -> Option<&Id> {
        self.stack
            .iter()
            .rev()
            .find(|context_id| !context_id.duplicate)
            .map(|context_id| &context_id.id)
    }
}

thread_local! {
    static CONTEXT: RefCell<SpanStack> = RefCell::new(SpanStack::new());
}

macro_rules! debug_panic {
    ($($args:tt)*) => {
        #[cfg(debug_assertions)] {
            if !std::thread::panicking() {
                panic!($($args)*)
            }
        }
    }
}

// ===== impl Span =====

impl<'a> Span<'a> {
    pub fn name(&self) -> &'static str {
        match self.lock.span {
            State::Full(ref data) => data.metadata.name(),
            State::Empty(_) => unreachable!(),
        }
    }

    pub fn metadata(&self) -> &'static Metadata<'static> {
        match self.lock.span {
            State::Full(ref data) => data.metadata,
            State::Empty(_) => unreachable!(),
        }
    }

    pub fn fields(&self) -> &str {
        self.lock.fields.as_ref()
    }

    pub fn parent(&self) -> Option<&Id> {
        match self.lock.span {
            State::Full(ref data) => data.parent.as_ref(),
            State::Empty(_) => unreachable!(),
        }
    }

    #[inline(always)]
    fn with_parent<'store, F, E>(
        self,
        my_id: &Id,
        last_id: Option<&Id>,
        f: &mut F,
        store: &'store Store,
    ) -> Result<(), E>
    where
        F: FnMut(&Id, Span<'_>) -> Result<(), E>,
    {
        if let Some(parent_id) = self.parent() {
            if Some(parent_id) != last_id {
                if let Some(parent) = store.get(parent_id) {
                    parent.with_parent(parent_id, Some(my_id), f, store)?;
                } else {
                    debug_panic!("missing span for {:?}; this is a bug", parent_id);
                }
            }
        }
        f(my_id, self)
    }
}

impl<'a> fmt::Debug for Span<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Span")
            .field("name", &self.name())
            .field("parent", &self.parent())
            .field("metadata", self.metadata())
            .field("fields", &self.fields())
            .finish()
    }
}

// ===== impl Context =====

impl<'a, F> Context<'a, F> {
    /// Applies a function to each span in the current trace context.
    ///
    /// The function is applied in order, beginning with the root of the trace,
    /// and ending with the current span. If the function returns an error,
    /// this will short-circuit.
    ///
    /// If invoked from outside of a span, the function will not be applied.
    ///
    /// Note that if we are currently unwinding, this will do nothing, rather
    /// than potentially causing a double panic.
    pub fn visit_spans<N, E>(&self, mut f: N) -> Result<(), E>
    where
        N: FnMut(&Id, Span<'_>) -> Result<(), E>,
    {
        CONTEXT
            .try_with(|current| {
                if let Some(id) = current.borrow().current() {
                    if let Some(span) = self.store.get(id) {
                        // with_parent uses the call stack to visit the span
                        // stack in reverse order, without having to allocate
                        // a buffer.
                        return span.with_parent(id, None, &mut f, self.store);
                    } else {
                        debug_panic!("missing span for {:?}; this is a bug", id);
                    }
                }
                Ok(())
            })
            .unwrap_or(Ok(()))
    }

    /// Executes a closure with the reference to the current span.
    pub fn with_current<N, R>(&self, f: N) -> Option<R>
    where
        N: FnOnce((&Id, Span<'_>)) -> R,
    {
        // If the lock is poisoned or the thread local has already been
        // destroyed, we might be in the middle of unwinding, so this
        // will just do nothing rather than cause a double panic.
        CONTEXT
            .try_with(|current| {
                if let Some(id) = current.borrow().current() {
                    if let Some(span) = self.store.get(&id) {
                        return Some(f((&id, span)));
                    } else {
                        debug_panic!("missing span for {:?}, this is a bug", id);
                    }
                }
                None
            })
            .ok()?
    }

    pub(crate) fn new(store: &'a Store, fmt_fields: &'a F) -> Self {
        Self { store, fmt_fields }
    }
}

impl<'ctx, 'writer, F> FormatFields<'writer> for Context<'ctx, F>
where
    F: FormatFields<'writer>,
{
    #[inline]
    fn format_fields<R>(&self, writer: &'writer mut dyn fmt::Write, fields: R) -> fmt::Result
    where
        R: RecordFields,
    {
        self.fmt_fields.format_fields(writer, fields)
    }
}

#[inline]
fn idx_to_id(idx: usize) -> Id {
    Id::from_u64(idx as u64 + 1)
}

#[inline]
fn id_to_idx(id: &Id) -> usize {
    id.into_u64() as usize - 1
}

impl Store {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Store {
            inner: RwLock::new(Slab {
                slab: Vec::with_capacity(capacity),
            }),
            next: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub(crate) fn current(&self) -> Option<Id> {
        CONTEXT
            .try_with(|current| current.borrow().current().cloned())
            .ok()?
    }

    pub(crate) fn push(&self, id: &Id) {
        let _ = CONTEXT.try_with(|current| current.borrow_mut().push(self.clone_span(id)));
    }

    pub(crate) fn pop(&self, expected_id: &Id) {
        let id = CONTEXT
            .try_with(|current| current.borrow_mut().pop(expected_id))
            .ok()
            .and_then(|i| i);
        if let Some(id) = id {
            let _ = self.drop_span(id);
        }
    }

    /// Inserts a new span with the given data and fields into the slab,
    /// returning an ID for that span.
    ///
    /// If there are empty slots in the slab previously allocated for spans
    /// which have since been closed, the allocation and span ID of the most
    /// recently emptied span will be reused. Otherwise, a new allocation will
    /// be added to the slab.
    #[inline]
    pub(crate) fn new_span<F>(&self, attrs: &Attributes<'_>, fmt_fields: &F) -> Id
    where
        F: for<'writer> FormatFields<'writer>,
    {
        let mut span = Some(Data::new(attrs, self));

        // The slab's free list is a modification of Treiber's lock-free stack,
        // using slab indices instead of pointers, and with a provision for
        // growing the slab when needed.
        //
        // In order to insert a new span into the slab, we "pop" the next free
        // index from the stack.
        loop {
            // Acquire a snapshot of the head of the free list.
            let head = self.next.load(Ordering::Relaxed);

            {
                // Try to insert the span without modifying the overall
                // structure of the stack.
                let this = try_lock!(self.inner.read(), else return Id::from_u64(0xDEADFACE));

                // Can we insert without reallocating?
                if head < this.slab.len() {
                    // If someone else is writing to the head slot, we need to
                    // acquire a new snapshot!
                    if let Ok(mut slot) = this.slab[head].try_write() {
                        // Is the slot we locked actually empty? If not, fall
                        // through and try to grow the slab.
                        if let Some(next) = slot.next() {
                            // Is our snapshot still valid?
                            if self.next.compare_and_swap(head, next, Ordering::Release) == head {
                                // We can finally fill the slot!
                                slot.fill(span.take().unwrap(), attrs, fmt_fields);
                                return idx_to_id(head);
                            }
                        }
                    }

                    // Our snapshot got stale, try again!
                    atomic::spin_loop_hint();
                    continue;
                }
            }

            // We need to grow the slab, and must acquire a write lock.
            if let Ok(mut this) = self.inner.try_write() {
                let len = this.slab.len();

                // Insert the span into a new slot.
                let slot = Slot::new(span.take().unwrap(), attrs, fmt_fields);
                this.slab.push(RwLock::new(slot));
                // TODO: can we grow the slab in chunks to avoid having to
                // realloc as often?

                // Update the head pointer and return.
                self.next.store(len + 1, Ordering::Release);
                return idx_to_id(len);
            }

            atomic::spin_loop_hint();
        }
    }

    /// Returns a `Span` to the span with the specified `id`, if one
    /// currently exists.
    #[inline]
    pub(crate) fn get(&self, id: &Id) -> Option<Span<'_>> {
        let read = try_lock!(self.inner.read(), else return None);
        let lock = OwningHandle::try_new(read, |slab| {
            unsafe { &*slab }.read_slot(id_to_idx(id)).ok_or(())
        })
        .ok()?;
        Some(Span { lock })
    }

    /// Records that the span with the given `id` has the given `fields`.
    #[inline]
    pub(crate) fn record<F>(&self, id: &Id, fields: &Record<'_>, fmt_fields: &F)
    where
        F: for<'writer> FormatFields<'writer>,
    {
        let slab = try_lock!(self.inner.read(), else return);
        let slot = slab.write_slot(id_to_idx(id));
        if let Some(mut slot) = slot {
            slot.record(fields, fmt_fields);
        }
    }

    /// Decrements the reference count of the span with the given `id`, and
    /// removes the span if it is zero.
    ///
    /// The allocated span slot will be reused when a new span is created.
    pub(crate) fn drop_span(&self, id: Id) -> bool {
        let this = try_lock!(self.inner.read(), else return false);
        let idx = id_to_idx(&id);

        if !this
            .slab
            .get(idx)
            .and_then(|lock| {
                let span = try_lock!(lock.read(), else return None);
                Some(span.drop_ref())
            })
            .unwrap_or_else(|| {
                debug_panic!("tried to drop {:?} but it no longer exists!", id);
                false
            })
        {
            return false;
        }

        // Synchronize only if we are actually removing the span (stolen
        // from std::Arc);
        atomic::fence(Ordering::Acquire);

        this.remove(&self.next, idx);
        true
    }

    pub(crate) fn clone_span(&self, id: &Id) -> Id {
        let this = try_lock!(self.inner.read(), else return id.clone());
        let idx = id_to_idx(id);

        if let Some(span) = this.slab.get(idx).and_then(|span| span.read().ok()) {
            span.clone_ref();
        } else {
            debug_panic!(
                "tried to clone {:?}, but no span exists with that ID. this is a bug!",
                id
            );
        }
        id.clone()
    }
}

impl Data {
    pub(crate) fn new(attrs: &Attributes<'_>, store: &Store) -> Self {
        let parent = if attrs.is_root() {
            None
        } else if attrs.is_contextual() {
            store.current().as_ref().map(|id| store.clone_span(id))
        } else {
            attrs.parent().map(|id| store.clone_span(id))
        };
        Self {
            metadata: attrs.metadata(),
            parent,
            ref_count: AtomicUsize::new(1),
            is_empty: true,
        }
    }
}

impl Drop for Data {
    fn drop(&mut self) {
        // We have to actually unpack the option inside the `get_default`
        // closure, since it is a `FnMut`, but testing that there _is_ a value
        // here lets us avoid the thread-local access if we don't need the
        // dispatcher at all.
        if self.parent.is_some() {
            dispatcher::get_default(|subscriber| {
                if let Some(parent) = self.parent.take() {
                    let _ = subscriber.try_close(parent);
                }
            })
        }
    }
}

impl Slot {
    fn new<F>(mut data: Data, attrs: &Attributes<'_>, fmt_fields: &F) -> Self
    where
        F: for<'writer> FormatFields<'writer>,
    {
        let mut fields = String::new();
        fmt_fields
            .format_fields(&mut fields, attrs)
            .expect("formatting to string should not fail");
        if fields.is_empty() {
            data.is_empty = false;
        }
        Self {
            fields,
            span: State::Full(data),
        }
    }

    fn next(&self) -> Option<usize> {
        match self.span {
            State::Empty(next) => Some(next),
            _ => None,
        }
    }

    fn fill<F>(&mut self, mut data: Data, attrs: &Attributes<'_>, fmt_fields: &F) -> usize
    where
        F: for<'writer> FormatFields<'writer>,
    {
        let fields = &mut self.fields;
        fmt_fields
            .format_fields(fields, attrs)
            .expect("formatting to string should not fail");
        if fields.is_empty() {
            data.is_empty = false;
        }
        match mem::replace(&mut self.span, State::Full(data)) {
            State::Empty(next) => next,
            State::Full(_) => unreachable!("tried to fill a full slot"),
        }
    }

    fn record<F>(&mut self, fields: &Record<'_>, fmt_fields: &F)
    where
        F: for<'writer> FormatFields<'writer>,
    {
        let state = &mut self.span;
        let buf = &mut self.fields;
        match state {
            State::Empty(_) => return,
            State::Full(ref mut data) => {
                fmt_fields
                    .format_fields(buf, fields)
                    .expect("formatting to string should not fail");
                if buf.is_empty() {
                    data.is_empty = false;
                }
            }
        }
    }

    fn drop_ref(&self) -> bool {
        match self.span {
            State::Full(ref data) => {
                let refs = data.ref_count.fetch_sub(1, Ordering::Release);
                debug_assert!(
                    if std::thread::panicking() {
                        // don't cause a double panic, even if the ref-count is wrong...
                        true
                    } else {
                        refs != std::usize::MAX
                    },
                    "reference count overflow!"
                );
                refs == 1
            }
            State::Empty(_) => false,
        }
    }

    fn clone_ref(&self) {
        match self.span {
            State::Full(ref data) => {
                let _refs = data.ref_count.fetch_add(1, Ordering::Release);
                debug_assert!(_refs != 0, "tried to clone a span that already closed!");
            }
            State::Empty(_) => {
                unreachable!("tried to clone a ref to a span that no longer exists, this is a bug")
            }
        }
    }
}

impl Slab {
    #[inline]
    fn write_slot(&self, idx: usize) -> Option<RwLockWriteGuard<'_, Slot>> {
        self.slab.get(idx).and_then(|slot| slot.write().ok())
    }

    #[inline]
    fn read_slot(&self, idx: usize) -> Option<RwLockReadGuard<'_, Slot>> {
        self.slab
            .get(idx)
            .and_then(|slot| slot.read().ok())
            .and_then(|lock| match lock.span {
                State::Empty(_) => None,
                State::Full(_) => Some(lock),
            })
    }

    /// Remove a span slot from the slab.
    fn remove(&self, next: &AtomicUsize, idx: usize) -> Option<Data> {
        // Again we are essentially implementing a variant of Treiber's stack
        // algorithm to push the removed span's index into the free list.
        loop {
            // Get a snapshot of the current free-list head.
            let head = next.load(Ordering::Relaxed);

            // Empty the data stored at that slot.
            let mut slot = try_lock!(self.slab[idx].write(), else return None);
            let data = match mem::replace(&mut slot.span, State::Empty(head)) {
                State::Full(data) => data,
                state => {
                    // The slot has already been emptied; leave
                    // everything as it was and return `None`!
                    slot.span = state;
                    return None;
                }
            };

            // Is our snapshot still valid?
            if next.compare_and_swap(head, idx, Ordering::Release) == head {
                // Empty the string but retain the allocated capacity
                // for future spans.
                slot.fields.clear();
                return Some(data);
            }

            atomic::spin_loop_hint();
        }
    }
}
