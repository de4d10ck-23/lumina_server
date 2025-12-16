require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { createClient } = require("@supabase/supabase-js");
const multer = require("multer");
const path = require("path");
const https = require("https");
const fs = require("fs");

const app = express();
const port = process.env.PORT || 5000;

// Enable CORS
app.use(cors());
app.use(express.json({ limit: "50mb" }));
// // app.use("/upload/bookcover", express.static(path.join(__dirname, "upload", "bookcover"))); // Replaced by dynamic endpoint below

// API endpoint to serve book cover images dynamically
app.get("/api/bookcovers/:filename", (req, res) => {
  const filename = req.params.filename;
  const filePath = path.join(__dirname, "upload", "bookcover", filename);

  //console.log(`[Image API] Request for filename: ${filename}`);
  //console.log(`[Image API] Attempting to send file from path: ${filePath}`);

  res.sendFile(filePath, (err) => {
    if (err) {
      console.error(`[Image API] Error sending file ${filename}:`, err);
      // Respond with a 404 and a generic 'not found' message
      // Do not send the error object itself to the client for security reasons
      res
        .status(404)
        .json({ error: "Book cover not found or could not be accessed" });
    } else {
      //console.log(`[Image API] Successfully sent file: ${filename}`);
    }
  });
}); // Replaced by dynamic endpoint below

// API endpoint to serve book cover images dynamically
app.get("/api/bookcover/:filename", (req, res) => {
  const filename = req.params.filename;
  const filePath = path.join(__dirname, "upload", "bookcover", filename);

  res.sendFile(filePath, (err) => {
    if (err) {
      console.error("Error sending file:", err);
      res.status(404).json({ error: "Book cover not found" });
    }
  });
});

// Multer Configuration for image uploads
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, "upload/bookcover"); // Destination folder for uploads
  },
  filename: function (req, file, cb) {
    cb(null, Date.now() + path.extname(file.originalname)); // Unique filename based on timestamp and original extension
  },
});
const upload = multer({ storage: storage });

// --- SUPABASE CLIENTS ---
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);
// Admin client for system tasks (Notification sending, Reports)
const supabaseAdmin = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// --- MIDDLEWARE: AUTHENTICATION ---
const verifyUser = async (req, res, next) => {
  const token = req.headers.authorization?.split(" ")[1];
  if (!token) return res.status(401).json({ error: "Missing token" });

  const {
    data: { user },
    error,
  } = await supabase.auth.getUser(token);
  if (error || !user) return res.status(401).json({ error: "Invalid token" });

  req.user = user;
  next();
};

// Image Upload Endpoint
app.post("/api/upload/bookcover", upload.single("coverImage"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file uploaded." });
  }
  // Return the path where the image is stored
  res.json({ cover_url: req.file.filename });
});

// Multer Configuration for profile image uploads
const profileStorage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, "upload/profile"); // Destination folder for uploads
  },
  filename: function (req, file, cb) {
    cb(null, Date.now() + path.extname(file.originalname)); // Unique filename
  },
});
const uploadProfile = multer({ storage: profileStorage });

// Profile Image Upload Endpoint
app.post(
  "/api/upload/profile",
  uploadProfile.single("profileImage"),
  (req, res) => {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded." });
    }
    res.json({ avatar_url: req.file.filename });
  }
);

// API endpoint to serve profile images dynamically
app.get("/api/profile/:filename", (req, res) => {
  const filename = req.params.filename;
  const filePath = path.join(__dirname, "upload", "profile", filename);

  res.sendFile(filePath, (err) => {
    if (err) {
      console.error("Error sending file:", err);
      res.status(404).json({ error: "Profile image not found" });
    }
  });
});

// ==========================================
//               1. CORE USER & PROFILE
// ==========================================

// Get My Profile (with Notifications count)
app.get("/api/me", verifyUser, async (req, res) => {
  const { data: profile } = await supabaseAdmin
    .from("profiles")
    .select("*, birthDate:birth_date")
    .eq("id", req.user.id)
    .single();

  // Get unread notifications count
  const { count } = await supabaseAdmin
    .from("notifications")
    .select("*", { count: "exact", head: true })
    .eq("user_id", req.user.id)
    .eq("is_read", false);

  res.json({ ...profile, unread_notifications: count });
});

// Update Profile (Bio, Gender for demographics, Preferences)
app.put("/api/me", verifyUser, async (req, res) => {
  const { username, bio, gender, preferences, avatar_url, birthDate, email } =
    req.body;

  const { data, error } = await supabaseAdmin
    .from("profiles")
    .upsert({
      id: req.user.id,
      username,
      bio,
      gender,
      preferences,
      avatar_url,
      birth_date: birthDate,
      email,
    })
    .select()
    .single();

  if (error) {
    console.error(error);
    return res.status(500).json({ error: error.message });
  }
  res.json(data);
});

// Get Public User Profile (for "Following" pages)
app.get("/api/users/:username", async (req, res) => {
  const { data: profile, error } = await supabase
    .from("profiles")
    .select("id, username, avatar_url, bio, role, created_at")
    .eq("username", req.params.username)
    .single();

  if (error) return res.status(404).json({ error: "User not found" });

  // Get followers
  const { data: followers, error: followersError } = await supabase
    .from("follows")
    .select("follower_id")
    .eq("following_id", profile.id);

  // Get following
  const { data: following, error: followingError } = await supabase
    .from("follows")
    .select("following_id")
    .eq("follower_id", profile.id);

  if (followersError || followingError) {
    return res
      .status(500)
      .json({ error: "Could not fetch followers/following data" });
  }

  const { created_at, ...rest } = profile;
  const userProfile = {
    ...rest,
    followers: followers.map((f) => f.follower_id),
    following: following.map((f) => f.following_id),
    createdAt: created_at,
  };

  res.json(userProfile);
});

// Follow / Unfollow User
app.post("/api/users/:targetId/toggle-follow", verifyUser, async (req, res) => {
  const { targetId } = req.params;
  const followerId = req.user.id;

  // Check if already following
  const { data: existing } = await supabase
    .from("follows")
    .select("*")
    .match({ follower_id: followerId, following_id: targetId })
    .single();

  if (existing) {
    await supabase
      .from("follows")
      .delete()
      .match({ follower_id: followerId, following_id: targetId });
    res.json({ status: "unfollowed" });
  } else {
    await supabase
      .from("follows")
      .insert({ follower_id: followerId, following_id: targetId });

    // Send Notification
    await supabaseAdmin.from("notifications").insert({
      user_id: targetId,
      type: "follow",
      title: "New Follower",
      message: "Someone started following you!",
      link_url: `/profile/${followerId}`,
    });
    res.json({ status: "followed" });
  }
});

// New endpoint to get multiple users by their IDs
app.post("/api/users/bulk", async (req, res) => {
  const { ids } = req.body;

  if (!ids || !Array.isArray(ids)) {
    return res
      .status(400)
      .json({ error: "Invalid request, 'ids' array not provided." });
  }

  const { data, error } = await supabase
    .from("profiles")
    .select("id, username, avatar_url, role, created_at")
    .in("id", ids);

  if (error) {
    console.error("Bulk user fetch error:", error);
    return res.status(500).json({ error: "Failed to fetch user profiles." });
  }

  // The frontend `User` type expects `name` and `avatar`. Let's map the DB fields.
  const users = data.map((profile) => ({
    id: profile.id,
    name: profile.username, // Map username to name
    avatar: profile.avatar_url, // Map avatar_url to avatar
    role: profile.role,
    createdAt: profile.created_at,
  }));

  res.json(users);
});

// ==========================================
//           2. BOOKS & LIBRARY (HYBRID)
// ==========================================

// Get All Genres
app.get("/api/genres", async (req, res) => {
  try {
    console.log("[API] /api/genres request");
    const { data, error } = await supabase.from("genres").select("*");
    if (error) {
      console.error("[API] Error fetching genres:", error);
      return res.status(500).json({ error: error.message });
    }
    console.log(`[API] /api/genres - Found ${data?.length || 0} genres`);
    res.json(data || []);
  } catch (e) {
    console.error("[API] Unexpected error in /api/genres:", e);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get All Books (With Search, Genre Filter, Pagination)
app.get("/api/books", async (req, res) => {
  const { search, genre, page = 1 } = req.query;
  const from = (page - 1) * 20;
  const to = from + 19;

  console.log(
    `[API] /api/books request - page: ${page}, search: ${
      search || "none"
    }, genre: ${genre || "none"}`
  );

  let query = supabase
    .from("books")
    .select(
      `
            *, 
            profiles:author_id(username),
            book_genres(genres(name, slug)),
            chapters(*)
        `
    )
    .eq("status", "published")
    .order("created_at", { ascending: false })
    .range(from, to);

  if (search) query = query.ilike("title", `%${search}%`);

  let { data, error } = await query;

  // Filter by genre after fetching (client-side filtering for junction table)
  if (genre && data) {
    data = data.filter((book) =>
      book.book_genres?.some(
        (bg) => bg.genres?.slug?.toLowerCase() === genre.toLowerCase()
      )
    );
  }
  if (error) {
    console.error(`[API] Error fetching books:`, error);
    return res.status(500).json({ error: error.message });
  }

  console.log(`[API] /api/books - Found ${data?.length || 0} books`);

  // Determine library membership if a token was provided
  const token = req.headers.authorization?.split(" ")[1];
  let ownedSet = new Set();
  let purchasedSet = new Set();
  if (token) {
    try {
      const {
        data: { user },
      } = await supabase.auth.getUser(token);
      if (user) {
        const { data: entries } = await supabaseAdmin
          .from("library")
          .select("book_id")
          .eq("user_id", user.id);
        if (entries) ownedSet = new Set(entries.map((e) => e.book_id));
        // Also determine purchased books for this user among the current page
        const ids = (data || []).map((b) => b.id).filter(Boolean);
        if (ids.length) {
          const { data: txs } = await supabaseAdmin
            .from("transactions")
            .select("book_id")
            .eq("buyer_id", user.id)
            .in("book_id", ids);
          if (txs) purchasedSet = new Set(txs.map((t) => t.book_id));
        }
      }
    } catch (e) {
      console.warn("Could not determine library membership:", e.message || e);
    }
  }

  const cleaned = data.map((b) => {
    const publishedChapters = (b.chapters || []).filter((c) => c.is_published);
    const primaryGenre = b.book_genres?.[0]?.genres?.name || "Fiction";

    return {
      id: b.id,
      title: b.title,
      author: b.profiles?.username || "Unknown Author",
      price: b.price || 0,
      coverUrl: b.cover_url || "",
      category: primaryGenre,
      rating_avg: b.rating_avg || 0,
      description: b.description || "",
      publisherId: b.author_id,
      views_count: b.views_count || 0,
      chapters_count: publishedChapters.length,
      chapters: publishedChapters,
      reviews: [], // Initialize as empty - will be fetched separately if needed
      status: b.status || "published",
      genres: b.book_genres.map((bg) => bg.genres?.name).filter(Boolean),
      isInLibrary: ownedSet.has(b.id),
      isPurchased: purchasedSet.has(b.id),
    };
  });
  // Log payload sent to client for debugging when the books list is requested
  try {
    console.log(
      `[API] /api/books payload (count=${cleaned.length}):`,
      JSON.stringify(cleaned, null, 2)
    );
  } catch (e) {
    console.log(
      `[API] /api/books payload (non-serializable) count=${cleaned.length}:`,
      cleaned
    );
  }

  res.json(cleaned);
});

// Get Total Count of Published Books
app.get("/api/books/stats/published-count", async (req, res) => {
  try {
    // Fetch all books and count those with status matching 'published' (case-insensitive)
    const { data, error } = await supabase.from("books").select("id, status");

    if (error) {
      console.error("[API] Error fetching books:", error);
      return res
        .status(500)
        .json({ error: "Failed to fetch published books count" });
    }

    // Count books with status 'published' (case-insensitive)
    const publishedCount = (data || []).filter(
      (book) => book.status && book.status.toLowerCase() === "published"
    ).length;

    console.log(
      "[API] Total published books count:",
      publishedCount,
      "from",
      data?.length || 0,
      "total books"
    );
    res.json({ total_published: publishedCount || 0 });
  } catch (err) {
    console.error(
      "[API] Unexpected error fetching published books count:",
      err
    );
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get Single Book by ID
app.get("/api/books/:id", async (req, res) => {
  const { id } = req.params;
  const { data, error } = await supabase
    .from("books")
    .select(
      `
      *,
      profiles:author_id(username),
      book_genres(genres(name, slug)),
      chapters(*)
    `
    )
    .eq("id", id)
    .single();

  if (error) {
    console.error(error);
    return res.status(404).json({ error: "Book not found" });
  }

  const publishedChapters = (data.chapters || []).filter((c) => c.is_published);
  const primaryGenre = data.book_genres?.[0]?.genres?.name || "Fiction";

  const cleaned = {
    id: data.id,
    title: data.title,
    author: data.profiles?.username || "Unknown Author",
    price: data.price || 0,
    coverUrl: data.cover_url || "",
    category: primaryGenre,
    rating_avg: data.rating_avg || 0,
    description: data.description || "",
    publisherId: data.author_id,
    views_count: data.views_count || 0,
    chapters_count: publishedChapters.length,
    chapters: publishedChapters,
    reviews: [],
    status: data.status || "published",
    genres: data.book_genres.map((bg) => bg.genres?.name).filter(Boolean),
  };
  // If an auth token is present, include whether this book is in the user's library
  const token = req.headers.authorization?.split(" ")[1];
  // Determine requester identity (authenticated user or anonymous visitor)
  let viewerId = null;
  let visitorIdentifier = null;
  if (token) {
    try {
      const {
        data: { user },
      } = await supabase.auth.getUser(token);
      if (user) viewerId = user.id;
    } catch (e) {
      console.warn("Could not get user from token:", e.message || e);
    }
  }

  // If no authenticated user, use IP (or x-forwarded-for) as anonymous identifier
  if (!viewerId) {
    visitorIdentifier = (req.headers["x-forwarded-for"] || req.ip || "")
      .toString()
      .split(",")[0]
      .trim();
    if (!visitorIdentifier) visitorIdentifier = null;
  }

  // Fetch library/purchase info when user is present
  if (viewerId) {
    try {
      const { data: existing } = await supabaseAdmin
        .from("library")
        .select("id")
        .match({ user_id: viewerId, book_id: id })
        .single();
      cleaned.isInLibrary = !!existing;
      const { data: tx } = await supabaseAdmin
        .from("transactions")
        .select("id")
        .match({ buyer_id: viewerId, book_id: id })
        .single();
      cleaned.isPurchased = !!tx;
    } catch (e) {
      console.warn(
        "Could not check library membership for book:",
        e.message || e
      );
      cleaned.isInLibrary = false;
      cleaned.isPurchased = false;
    }
  } else {
    cleaned.isInLibrary = false;
    cleaned.isPurchased = false;
  }

  // Record a single view per identity (user or anonymous) and increment views_count
  try {
    if (viewerId) {
      const { data: existingView } = await supabaseAdmin
        .from("book_views")
        .select("id")
        .match({ book_id: id, viewer_id: viewerId })
        .single();
      if (!existingView) {
        await supabaseAdmin.from("book_views").insert({
          book_id: id,
          viewer_id: viewerId,
        });
        const { data: bookRow } = await supabaseAdmin
          .from("books")
          .select("views_count")
          .eq("id", id)
          .single();
        const current = (bookRow && bookRow.views_count) || 0;
        await supabaseAdmin
          .from("books")
          .update({ views_count: Number(current) + 1 })
          .eq("id", id);
      }
    } else if (visitorIdentifier) {
      const { data: existingView } = await supabaseAdmin
        .from("book_views")
        .select("id")
        .match({ book_id: id, visitor_identifier: visitorIdentifier })
        .single();
      if (!existingView) {
        await supabaseAdmin.from("book_views").insert({
          book_id: id,
          visitor_identifier: visitorIdentifier,
        });
        const { data: bookRow } = await supabaseAdmin
          .from("books")
          .select("views_count")
          .eq("id", id)
          .single();
        const current = (bookRow && bookRow.views_count) || 0;
        await supabaseAdmin
          .from("books")
          .update({ views_count: Number(current) + 1 })
          .eq("id", id);
      }
    }
  } catch (e) {
    console.warn("Could not record view:", e.message || e);
  }

  res.json(cleaned);
});

// Get aggregated stats (total sales amount and total views) for all books by the authenticated publisher
app.get("/api/publisher/books/stats", verifyUser, async (req, res) => {
  try {
    // 1) Get book ids authored by this user
    const { data: myBooks } = await supabaseAdmin
      .from("books")
      .select("id")
      .eq("author_id", req.user.id);

    const ids = (myBooks || []).map((b) => b.id).filter(Boolean);
    if (!ids.length) return res.json([]);

    // 2) Fetch transactions for these books
    const { data: txs } = await supabaseAdmin
      .from("transactions")
      .select("book_id, amount")
      .in("book_id", ids)
      .eq("payment_status", "completed");

    // 3) Fetch views for these books
    const { data: views } = await supabaseAdmin
      .from("book_views")
      .select("book_id")
      .in("book_id", ids);

    const statsMap = {};
    ids.forEach((i) => {
      statsMap[i] = { totalSalesAmount: 0, totalSalesCount: 0, totalViews: 0 };
    });

    (txs || []).forEach((t) => {
      const bid = t.book_id;
      if (!statsMap[bid])
        statsMap[bid] = {
          totalSalesAmount: 0,
          totalSalesCount: 0,
          totalViews: 0,
        };
      statsMap[bid].totalSalesAmount += Number(t.amount || 0);
      statsMap[bid].totalSalesCount += 1;
    });

    (views || []).forEach((v) => {
      const bid = v.book_id;
      if (!statsMap[bid])
        statsMap[bid] = {
          totalSalesAmount: 0,
          totalSalesCount: 0,
          totalViews: 0,
        };
      statsMap[bid].totalViews += 1;
    });

    const result = Object.keys(statsMap).map((k) => ({
      book_id: Number(k),
      ...statsMap[k],
    }));
    res.json(result);
  } catch (e) {
    console.error("Error fetching publisher stats:", e);
    res.status(500).json({ error: e.message || "Server error" });
  }
});

// Get Publisher Financial Summary (Total Revenue & Performance Stats)
app.get("/api/publisher/revenue", verifyUser, async (req, res) => {
  try {
    const userId = req.user.id;
    console.log("[Revenue API] Fetching revenue for user:", userId);

    // Method 1: Get all book IDs for this publisher
    const { data: myBooks, error: booksError } = await supabaseAdmin
      .from("books")
      .select("id")
      .eq("author_id", userId);

    console.log(
      "[Revenue API] Books query - Error:",
      booksError,
      "Books found:",
      myBooks?.length
    );

    const bookIds = (myBooks || []).map((b) => b.id).filter(Boolean);
    console.log("[Revenue API] Book IDs:", bookIds);

    let totalRevenue = 0;
    let totalSalesCount = 0;
    let totalReads = 0;

    if (bookIds.length > 0) {
      // Get all completed transactions for this publisher's books
      const { data: txs, error: txError } = await supabaseAdmin
        .from("transactions")
        .select("*")
        .in("book_id", bookIds)
        .eq("payment_status", "completed");

      console.log(
        "[Revenue API] Transactions query - Error:",
        txError,
        "Transactions found:",
        txs?.length
      );
      console.log("[Revenue API] Transaction data:", txs);

      if (txs && txs.length > 0) {
        totalRevenue = txs.reduce((sum, tx) => sum + Number(tx.amount || 0), 0);
        totalSalesCount = txs.length;
        console.log(
          "[Revenue API] Calculated totalRevenue:",
          totalRevenue,
          "Count:",
          totalSalesCount
        );
      }

      // Get total views/reads for all books
      const { data: viewData, error: viewError } = await supabaseAdmin
        .from("book_views")
        .select("id")
        .in("book_id", bookIds);

      console.log(
        "[Revenue API] Views query - Error:",
        viewError,
        "Views found:",
        viewData?.length
      );

      if (viewData) {
        totalReads = viewData.length;
      }
    } else {
      console.log("[Revenue API] No books found for this user");
    }

    // Available balance is the total revenue (70% available, 30% held)
    const availableBalance = totalRevenue * 0.7;

    const responseData = {
      totalRevenue: Number(totalRevenue.toFixed(2)),
      availableBalance: Number(availableBalance.toFixed(2)),
      totalSalesCount: totalSalesCount,
      totalReads: totalReads,
    };

    console.log("[Revenue API] Sending response:", responseData);
    res.json(responseData);
  } catch (e) {
    console.error("Error fetching publisher revenue:", e);
    res.status(500).json({
      error: "Failed to fetch revenue data",
      totalRevenue: 0,
      availableBalance: 0,
      totalSalesCount: 0,
      totalReads: 0,
    });
  }
});

// Get top earning books for publisher
app.get("/api/publisher/top-books", verifyUser, async (req, res) => {
  try {
    const userId = req.user.id;
    console.log("[Top Books API] Fetching top books for user:", userId);

    // Get all books for this publisher with their titles
    const { data: myBooks, error: booksError } = await supabaseAdmin
      .from("books")
      .select("id, title")
      .eq("author_id", userId);

    if (booksError || !myBooks || myBooks.length === 0) {
      console.log("[Top Books API] No books found");
      return res.json([]);
    }

    const bookIds = myBooks.map((b) => b.id).filter(Boolean);
    console.log("[Top Books API] Book IDs:", bookIds);

    // Get transactions for each book
    const { data: txs, error: txError } = await supabaseAdmin
      .from("transactions")
      .select("book_id, amount")
      .in("book_id", bookIds)
      .eq("payment_status", "completed");

    if (txError || !txs) {
      console.log("[Top Books API] No transactions found");
      return res.json([]);
    }

    // Group by book and sum earnings
    const bookEarnings = {};

    myBooks.forEach((book) => {
      bookEarnings[String(book.id)] = {
        title: book.title,
        earnings: 0,
      };
    });

    txs.forEach((tx) => {
      const bookId = String(tx.book_id);
      if (bookEarnings[bookId]) {
        bookEarnings[bookId].earnings += Number(tx.amount || 0);
      }
    });

    // Sort by earnings descending and get top 3
    const topBooks = Object.values(bookEarnings)
      .sort((a, b) => b.earnings - a.earnings)
      .slice(0, 3);

    console.log("[Top Books API] Top books:", topBooks);
    res.json(topBooks);
  } catch (e) {
    console.error("Error fetching top books:", e);
    res.status(500).json([]);
  }
});

// Publish a Book (Author Only)
app.post("/api/books", verifyUser, async (req, res) => {
  console.log("Received request to publish a book:");
  console.log(req.body);

  const {
    title,
    description,
    price,
    coverUrl, // This should be the path returned by the upload endpoint
    downloadUrl,
    genreIds,
    chapters,
    status = "published",
  } = req.body;

  // Validate coverUrl
  if (!coverUrl) {
    return res.status(400).json({ error: "Book cover image is required." });
  }
  if (coverUrl.startsWith("data:image")) {
    return res.status(400).json({
      error:
        "Book cover image must be uploaded via the /api/upload/bookcover endpoint. Please provide the image path, not a base64 string.",
    });
  }

  console.log("Final coverUrl for database insertion:", coverUrl); // Add this line
  // 1. Insert Book
  // Get the primary genre name from the first genreId
  let category = null;
  if (genreIds?.length > 0) {
    const { data: genreData, error: genreError } = await supabase
      .from("genres")
      .select("name")
      .eq("id", genreIds[0])
      .single();

    if (genreData) {
      category = genreData.name;
    }
  }

  const { data: book, error } = await supabaseAdmin
    .from("books")
    .insert({
      author_id: req.user.id,
      title,
      description,
      price,
      category: category, // Add this line
      cover_url: coverUrl,
      external_download_url: downloadUrl,
      status,
    })
    .select()
    .single();

  if (error) {
    console.error("Error inserting book:", error);
    return res.status(500).json({ error: error.message });
  }
  console.log("Successfully inserted book:", book);

  // 2. Insert Chapters
  if (chapters && chapters.length > 0) {
    const chapterData = chapters.map((chapter, index) => ({
      book_id: book.id,
      title: chapter.title,
      content: chapter.content,
      sequence_order: index + 1, // Add sequence_order
      is_free_preview: chapter.isFree,
      is_published: chapter.isPublished, // Map isPublished from frontend
    }));

    const { error: chapterError } = await supabaseAdmin
      .from("chapters")
      .insert(chapterData);

    if (chapterError) {
      console.error("Error inserting chapters:", chapterError);
      // We might want to handle this more gracefully, but for now, we'll just log it
    } else {
      console.log("Successfully inserted chapters");
    }
  }

  // 3. Link Genres
  if (genreIds?.length) {
    const links = genreIds.map((gid) => ({ book_id: book.id, genre_id: gid }));
    const { error: genreError } = await supabaseAdmin
      .from("book_genres")
      .insert(links);
    if (genreError) {
      console.error("Error linking genres:", genreError);
      // We might want to handle this more gracefully, but for now, we'll just log it
    } else {
      console.log("Successfully linked genres");
    }
  }

  // 4. Notify Followers
  const { data: followers, error: followersError } = await supabase
    .from("follows")
    .select("follower_id")
    .eq("following_id", req.user.id);

  if (followersError) {
    console.error("Error fetching followers:", followersError);
  } else if (followers?.length) {
    const notifs = followers.map((f) => ({
      user_id: f.follower_id,
      type: "new_book",
      title: "New Book Release!",
      message: `${title} is now available.`,
      link_url: `/book/${book.id}`,
    }));
    const { error: notificationError } = await supabaseAdmin
      .from("notifications")
      .insert(notifs);
    if (notificationError) {
      console.error("Error creating notifications:", notificationError);
    } else {
      console.log("Successfully created notifications for followers.");
    }
  }

  console.log("Sending back the new book data:", book);
  res.json(book);
});

// Add a library entry without payment (e.g., for freebies or manual adds)
app.post("/api/library/add", verifyUser, async (req, res) => {
  const { bookId } = req.body;
  if (!bookId) return res.status(400).json({ error: "Missing bookId" });

  try {
    // Use admin client to bypass row-level security when modifying server-side data
    // Check if already in library
    const { data: existing, error: existingError } = await supabaseAdmin
      .from("library")
      .select("*")
      .match({ user_id: req.user.id, book_id: bookId })
      .single();

    if (existing) {
      return res.json({ success: true, already: true });
    }

    const { error } = await supabaseAdmin.from("library").insert({
      user_id: req.user.id,
      book_id: bookId,
    });

    if (error) {
      console.error("Error adding to library:", error);
      return res.status(500).json({ error: error.message });
    }

    return res.status(201).json({ success: true, already: false });
  } catch (e) {
    console.error("Unexpected error adding to library:", e);
    return res.status(500).json({ error: "Unexpected server error" });
  }
});

// Get current user's library books
app.get("/api/library", verifyUser, async (req, res) => {
  try {
    const { data: entries, error: entriesError } = await supabaseAdmin
      .from("library")
      .select("book_id, created_at")
      .eq("user_id", req.user.id);

    if (entriesError) {
      console.error("Error fetching library entries:", entriesError);
      return res.status(500).json({ error: entriesError.message });
    }

    const ids = entries.map((e) => e.book_id);
    const addedAtMap = {};
    entries.forEach((e) => {
      if (e && e.book_id) addedAtMap[e.book_id] = e.created_at || null;
    });
    if (!ids.length) return res.json([]);

    const { data: books, error: booksError } = await supabase
      .from("books")
      .select(
        `
            *, 
            profiles:author_id(username),
            book_genres(genres(name, slug)),
            chapters(*)
        `
      )
      .in("id", ids);

    if (booksError) {
      console.error("Error fetching books for library:", booksError);
      return res.status(500).json({ error: booksError.message });
    }

    // Determine purchases for this user among the library books using owned_books
    const { data: owned } = await supabaseAdmin
      .from("owned_books")
      .select("book_id")
      .eq("user_id", req.user.id)
      .in("book_id", ids);
    const purchasedSet = new Set((owned || []).map((t) => t.book_id));

    const cleaned = books.map((b) => ({
      ...b,
      coverUrl: b.cover_url,
      author: b.profiles?.username,
      publisherId: b.author_id,
      genres: b.book_genres?.map((bg) => bg.genres?.name).filter(Boolean) || [],
      // Only expose published chapters
      chapters: (b.chapters || []).filter((c) => c.is_published),
      chapters_count:
        (b.chapters || []).filter((c) => c.is_published).length || 0,
      // Whether the current user purchased this book
      isPurchased: purchasedSet.has(b.id),
      // When user added this book to their library
      dateAdded: addedAtMap[b.id] || null,
    }));

    res.json(cleaned);
  } catch (e) {
    console.error("Unexpected error in /api/library:", e);
    res.status(500).json({ error: "Unexpected server error" });
  }
});

// Remove a book from user's library
app.delete("/api/library/:bookId", verifyUser, async (req, res) => {
  const { bookId } = req.params;
  if (!bookId) return res.status(400).json({ error: "Missing bookId" });

  try {
    const { error } = await supabaseAdmin
      .from("library")
      .delete()
      .match({ user_id: req.user.id, book_id: bookId });

    if (error) {
      console.error("Error removing from library:", error);
      return res.status(500).json({ error: error.message });
    }

    res.json({ success: true });
  } catch (e) {
    console.error("Unexpected error removing from library:", e);
    res.status(500).json({ error: "Unexpected server error" });
  }
});

// Get Reviews for a Book
app.get("/api/books/:id/reviews", async (req, res) => {
  try {
    const { id: bookIdParam } = req.params;
    const bookId = parseInt(bookIdParam, 10);

    console.log("[API] GET /api/books/:id/reviews");
    console.log("[API] Book ID from params:", bookIdParam);
    console.log("[API] Converted Book ID:", bookId);

    const { data, error } = await supabase
      .from("reviews")
      .select(
        `
        id,
        rating,
        comment,
        created_at,
        user_id,
        book_id,
        profiles(username, avatar_url)
      `
      )
      .eq("book_id", bookId)
      .order("created_at", { ascending: false });

    if (error) {
      console.error("[API] Error fetching reviews:", error);
      console.error("[API] Error details:", JSON.stringify(error));
      return res.status(500).json({ error: error.message });
    }

    console.log("[API] Reviews query result count:", data?.length || 0);

    // Format reviews for frontend
    const formattedReviews = (data || []).map((review) => ({
      id: review.id,
      userId: review.user_id,
      userName: review.profiles?.username || "Anonymous",
      userAvatar:
        review.profiles?.avatar_url || "https://via.placeholder.com/40",
      rating: review.rating || 0,
      content: review.comment || "",
      timestamp: new Date(review.created_at).toLocaleDateString(),
    }));

    console.log(
      `[API] Found ${formattedReviews.length} reviews for book ${bookId}`
    );
    res.json({ reviews: formattedReviews });
  } catch (err) {
    console.error("[API] Unexpected error fetching reviews:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get Average Rating for a Book
app.get("/api/books/:id/average-rating", async (req, res) => {
  try {
    const { id: bookId } = req.params;

    console.log("[API] GET /api/books/:id/average-rating for book:", bookId);

    const { data: reviews, error } = await supabase
      .from("reviews")
      .select("rating")
      .eq("book_id", bookId);

    if (error) {
      console.error("[API] Error fetching reviews for rating:", error);
      return res.status(500).json({ error: error.message });
    }

    // Calculate average rating
    const reviewsWithRatings = (reviews || []).filter((r) => r.rating > 0);
    const averageRating =
      reviewsWithRatings.length > 0
        ? reviewsWithRatings.reduce((sum, r) => sum + r.rating, 0) /
          reviewsWithRatings.length
        : 0;

    console.log(
      `[API] Average rating for book ${bookId}: ${averageRating.toFixed(2)} (${
        reviewsWithRatings.length
      } reviews)`
    );

    res.json({
      averageRating: parseFloat(averageRating.toFixed(2)),
      reviewCount: reviewsWithRatings.length,
    });
  } catch (err) {
    console.error("[API] Unexpected error calculating average rating:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Add Review / Comment
app.post("/api/books/:id/reviews", verifyUser, async (req, res) => {
  try {
    const { rating, comment } = req.body;
    const { id: bookIdParam } = req.params;
    const bookId = parseInt(bookIdParam, 10);
    const userId = req.user?.id;

    console.log("[API] POST /api/books/:id/reviews");
    console.log("[API] Book ID from params:", bookIdParam);
    console.log("[API] Converted Book ID:", bookId);
    console.log("[API] User ID:", userId);
    console.log("[API] Rating:", rating);
    console.log("[API] Comment length:", comment?.length);

    if (!userId) {
      console.error("[API] User ID not found");
      return res.status(401).json({ error: "User not authenticated" });
    }

    if (!bookId || isNaN(bookId)) {
      console.error("[API] Invalid Book ID:", bookIdParam);
      return res
        .status(400)
        .json({ error: "Book ID is required and must be a number" });
    }

    console.log("[API] Inserting review to Supabase...");
    const { data, error } = await supabase
      .from("reviews")
      .insert({
        user_id: userId,
        book_id: bookId,
        rating: parseInt(rating) || 0,
        comment: comment.trim(),
      })
      .select();

    if (error) {
      console.error("[API] Supabase error:", error);
      console.error("[API] Error details:", JSON.stringify(error));
      return res.status(500).json({ error: error.message || "Database error" });
    }

    console.log("[API] Review inserted successfully:", data);
    res.json({ success: true, review: data?.[0] });
  } catch (err) {
    console.error("[API] Unexpected error adding review:", err);
    res.status(500).json({ error: err.message || "Internal server error" });
  }
});

// ==========================================
//          3. READING & PROGRESS
// ==========================================

// Get Book Content (Reader Mode)
app.get("/api/read/:bookId", verifyUser, async (req, res) => {
  const { bookId } = req.params;

  // Check Ownership
  // First check owned_books (authoritative), then fall back to library entries
  let owned = null;
  try {
    const { data: ownedData } = await supabaseAdmin
      .from("owned_books")
      .select("id")
      .match({ user_id: req.user.id, book_id: bookId })
      .single();
    owned = ownedData;
  } catch (e) {
    // ignore and continue to check library
  }

  if (!owned) {
    const { data: libEntry } = await supabase
      .from("library")
      .select("id")
      .match({ user_id: req.user.id, book_id: bookId })
      .single();
    owned = libEntry;
  }

  // Fetch Chapters
  let query = supabase
    .from("chapters")
    .select("*")
    .eq("book_id", bookId)
    .order("sequence_order");
  // Only published chapters should be returned
  query = query.eq("is_published", true);
  if (!owned) query = query.eq("is_free_preview", true); // Only free chapters if not bought

  const { data: chapters } = await query;

  // Fetch User Progress (including completed chapters)
  const { data: progress } = await supabase
    .from("reading_progress")
    .select("progress_percentage, current_chapter_id, completed_chapter_ids")
    .match({ user_id: req.user.id, book_id: bookId })
    .single();

  const access = owned
    ? "full"
    : chapters && chapters.length > 0
    ? "preview"
    : "none";

  res.json({
    access,
    progress: progress || { progress_percentage: 0, completed_chapter_ids: [] },
    chapters: chapters || [],
  });
});

// Update Progress Bar
app.post("/api/progress", verifyUser, async (req, res) => {
  const { bookId, chapterId, percent, completedChapterId } = req.body;
  console.log(
    "[API] /api/progress called by user:",
    req.user?.id,
    "body:",
    req.body
  );

  try {
    // Fetch existing entry so we can merge completed_chapter_ids
    const { data: existing } = await supabase
      .from("reading_progress")
      .select("completed_chapter_ids")
      .match({ user_id: req.user.id, book_id: bookId })
      .single();

    let completed = (existing && existing.completed_chapter_ids) || [];
    const incomingCid = chapterId || completedChapterId;
    if (incomingCid) {
      const cid = Number(incomingCid);
      if (!completed.includes(cid)) completed = [...completed, cid];
    }

    // Get total published chapters for the book to compute percentage
    const { count } = await supabase
      .from("chapters")
      .select("id", { count: "exact", head: true })
      .eq("book_id", bookId)
      .eq("is_published", true);

    const total = Number(count) || 0;
    const computedPercent =
      total > 0 ? Math.round((completed.length / total) * 100) : 0;

    const { error } = await supabase.from("reading_progress").upsert({
      user_id: req.user.id,
      book_id: bookId,
      current_chapter_id: chapterId || null,
      progress_percentage: computedPercent,
      last_read_at: new Date(),
      completed_chapter_ids: completed,
    });

    if (error) return res.status(500).json({ error: error.message });

    res.json({
      success: true,
      progress: {
        progress_percentage: computedPercent,
        completed_chapter_ids: completed,
      },
    });
  } catch (e) {
    console.error("Error upserting reading_progress:", e);
    res.status(500).json({ error: "Failed to update progress" });
  }
});

// ==========================================
//        4. PAYMENTS & TRANSACTIONS
// ==========================================

app.post("/api/payment/confirm", verifyUser, async (req, res) => {
  const { bookId, paymentMethod } = req.body;

  // Basic card validation helper (Luhn)
  const luhnValid = (num) => {
    const s = String(num).replace(/\s+/g, "");
    let sum = 0;
    let shouldDouble = false;
    for (let i = s.length - 1; i >= 0; i--) {
      let digit = parseInt(s.charAt(i), 10);
      if (isNaN(digit)) return false;
      if (shouldDouble) {
        digit *= 2;
        if (digit > 9) digit -= 9;
      }
      sum += digit;
      shouldDouble = !shouldDouble;
    }
    return sum % 10 === 0;
  };

  // Validate input
  if (!bookId || !paymentMethod) {
    return res.status(400).json({ error: "Missing payment details" });
  }
  const { number, expiry, cvc, name } = paymentMethod;
  if (!number || !expiry || !cvc || !name) {
    return res.status(400).json({ error: "Incomplete card details" });
  }

  // Luhn check for card number (basic)
  if (!luhnValid(number)) {
    return res.status(400).json({ error: "Invalid card number" });
  }

  // expiry check MM/YY or MM/YYYY
  const expParts = String(expiry).split("/");
  if (expParts.length !== 2)
    return res.status(400).json({ error: "Invalid expiry format" });
  const month = parseInt(expParts[0], 10);
  let year = parseInt(expParts[1], 10);
  if (year < 100) year += 2000;
  const expDate = new Date(year, month - 1, 1);
  const now = new Date();
  if (isNaN(expDate.getTime()) || month < 1 || month > 12)
    return res.status(400).json({ error: "Invalid expiry date" });
  // Consider card expired at end of month
  const endOfMonth = new Date(
    expDate.getFullYear(),
    expDate.getMonth() + 1,
    0,
    23,
    59,
    59
  );
  if (endOfMonth < now) return res.status(400).json({ error: "Card expired" });

  if (!/^[0-9]{3,4}$/.test(String(cvc)))
    return res.status(400).json({ error: "Invalid CVC" });

  // 1. Get Book Details
  const { data: book } = await supabase
    .from("books")
    .select("price, author_id")
    .eq("id", bookId)
    .single();
  if (!book) return res.status(404).json({ error: "Book not found" });

  // 1b. Fetch Platform Settings for sales fee percentage
  let salesFeePercent = 20; // default 20% if not found
  const { data: platformSettings } = await supabase
    .from("platform_settings")
    .select("sales_fee_percent")
    .eq("id", "default")
    .single();

  if (platformSettings && platformSettings.sales_fee_percent !== undefined) {
    salesFeePercent = platformSettings.sales_fee_percent;
    console.log(
      `[PAYMENT] Using sales fee percentage from DB: ${salesFeePercent}%`
    );
  } else {
    console.log(
      `[PAYMENT] Platform settings not found, using default fee: ${salesFeePercent}%`
    );
  }

  // Calculate admin fee and author earning
  const adminFee = parseFloat(
    (book.price * (salesFeePercent / 100)).toFixed(2)
  );
  const authorEarning = parseFloat((book.price - adminFee).toFixed(2));

  console.log(
    `[PAYMENT] Book price: $${
      book.price
    }, Admin fee (${salesFeePercent}%): $${adminFee.toFixed(
      2
    )}, Author earning: $${authorEarning.toFixed(2)}`
  );

  // Prevent duplicate purchase: check if transaction already exists for this buyer/book
  // Use admin client for server-side checks/inserts to bypass RLS
  const { data: existingTx } = await supabaseAdmin
    .from("transactions")
    .select("id")
    .match({ buyer_id: req.user.id, book_id: bookId })
    .single();
  if (existingTx) {
    // ensure library entry exists
    await supabaseAdmin
      .from("library")
      .upsert({ user_id: req.user.id, book_id: bookId });
    return res.json({ success: true, already: true });
  }

  // 2. Create Transaction Record (dummy - always succeed if validations passed)
  const transactionPayload = {
    buyer_id: req.user.id,
    book_id: bookId,
    author_id: book.author_id,
    amount: book.price,
    type: "SALE",
    status: "COMPLETED",
    admin_fee: adminFee,
    author_earning: authorEarning,
    payment_status: "completed",
  };

  console.log(
    "[PAYMENT] Inserting transaction with payload:",
    JSON.stringify(transactionPayload)
  );
  console.log(
    `[PAYMENT] admin_fee value: ${adminFee}, type: ${typeof adminFee}`
  );

  const { data: tx, error: txError } = await supabaseAdmin
    .from("transactions")
    .insert(transactionPayload)
    .select()
    .single();

  if (txError) {
    console.error("[PAYMENT] Transaction insert error:", txError);
    console.error("[PAYMENT] Error code:", txError.code);
    console.error("[PAYMENT] Error details:", JSON.stringify(txError));
    return res.status(500).json({
      error: "Transaction failed",
      details: txError.message,
      code: txError.code,
    });
  }

  if (!tx) {
    console.error("[PAYMENT] No transaction data returned");
    return res
      .status(500)
      .json({ error: "Transaction failed - no data returned" });
  }

  console.log(
    "[PAYMENT] Transaction created - Returned admin_fee:",
    tx.admin_fee,
    "author_earning:",
    tx.author_earning
  );
  console.log("[PAYMENT] Full transaction object:", JSON.stringify(tx));

  // 3. Add to Library
  const { data: libraryRow, error: libraryError } = await supabaseAdmin
    .from("library")
    .insert({ user_id: req.user.id, book_id: bookId, created_at: new Date() })
    .select()
    .single();
  if (libraryError) console.error("library insert error:", libraryError);

  // 3b. Record ownership in owned_books (so owned_books is authoritative for purchases)
  try {
    const { data: ownedRow, error: ownedError } = await supabaseAdmin
      .from("owned_books")
      .insert({
        user_id: req.user.id,
        book_id: bookId,
        transaction_id: tx.id,
        acquired_at: new Date(),
      })
      .select()
      .single();
    if (ownedError) console.error("owned_books insert error:", ownedError);
  } catch (e) {
    // If owned_books insert fails, log but don't block the purchase flow
    console.error("Failed to record owned_books entry:", e);
  }

  // 4. Update Sales Counter (Atomic Increment)
  // Supabase doesn't have a simple "increment" API in JS, so we use an RPC or just let the dashboard count rows.
  // Ideally, you'd use a Database Function for this.

  // 5. Notify Author
  await supabaseAdmin.from("notifications").insert({
    user_id: book.author_id,
    type: "sale",
    title: "New Sale!",
    message: `You earned $${authorEarning.toFixed(
      2
    )}! (After ${salesFeePercent}% platform fee)`,
    link_url: "/dashboard/sales",
  });

  console.log(
    "[PAYMENT] tx:",
    tx,
    "libraryRow:",
    libraryRow,
    "ownedRow:",
    typeof ownedRow !== "undefined" ? ownedRow : null
  );

  res.json({
    success: true,
    bookId,
    transactionId: tx.id,
    library: libraryRow || null,
  });
});

// ==========================================
//       5. AUTHOR DASHBOARD & REPORTS
// ==========================================

app.get("/api/author/dashboard", verifyUser, async (req, res) => {
  const userId = req.user.id;

  // A. Financials
  const { data: txs } = await supabase
    .from("transactions")
    .select("amount, created_at")
    .eq("author_id", userId);

  const totalRevenue = txs.reduce((sum, t) => sum + Number(t.amount), 0);
  const totalSales = txs.length;

  // B. Audience Demographics (Gender Distribution)
  // We join Library -> Profiles to see who bought the books
  const { data: audience } = await supabase
    .from("library")
    .select(
      `
            book_id,
            profiles:user_id ( gender, preferences )
        `
    )
    .eq("book:books.author_id", userId); // Requires proper foreign key setup or deep filtering

  // Manual aggregation for demographics since Supabase JS doesn't do "GROUP BY" easily
  const demographics = { Male: 0, Female: 0, Other: 0 };
  audience?.forEach((entry) => {
    const g = entry.profiles?.gender || "Other";
    if (demographics[g] !== undefined) demographics[g]++;
    else demographics.Other++;
  });

  // C. Book Performance (Views vs Sales)
  const { data: books } = await supabase
    .from("books")
    .select("title, views_count, sales_count")
    .eq("author_id", userId);

  res.json({
    revenue: totalRevenue,
    sales: totalSales,
    demographics,
    bookStats: books,
  });
});

// Generate Report (CSV Data)
app.get("/api/author/report", verifyUser, async (req, res) => {
  const { data } = await supabase
    .from("transactions")
    .select(
      `
            created_at,
            amount,
            book:books(title),
            buyer:profiles(username, email)
        `
    )
    .eq("author_id", req.user.id)
    .order("created_at", { ascending: false });

  res.json({ report_data: data });
});

// ==========================================
//         6. MESSAGES & CHAT
// ==========================================

// Get conversation with a specific user
app.get("/api/messages/:otherId", verifyUser, async (req, res) => {
  const { otherId } = req.params;
  const myId = req.user.id;

  const { data, error } = await supabase
    .from("messages")
    .select("*")
    .or(
      `and(sender_id.eq.${myId},receiver_id.eq.${otherId}),and(sender_id.eq.${otherId},receiver_id.eq.${myId})`
    )
    .order("created_at", { ascending: true });

  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Send Message
app.post("/api/messages", verifyUser, async (req, res) => {
  const { receiverId, content } = req.body;

  const { error } = await supabase.from("messages").insert({
    sender_id: req.user.id,
    receiver_id: receiverId,
    content,
  });

  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ==========================================
//              7. MODERATION
// ==========================================

// Submit a Report (Flag a book or review)
app.post("/api/reports", verifyUser, async (req, res) => {
  try {
    const { targetId, targetType, reason, details } = req.body;

    if (!targetId || !targetType || !reason) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    const { data, error } = await supabase
      .from("reports")
      .insert({
        reporter_id: req.user.id,
        target_id: targetId,
        target_type: targetType, // 'book' or 'review'
        reason,
        details: details || "",
        status: "pending",
        created_at: new Date().toISOString(),
      })
      .select();

    if (error) {
      console.error("[API] Error submitting report:", error);
      return res.status(500).json({ error: error.message });
    }

    console.log("[API] Report submitted for", targetType, targetId);
    res.json({ message: "Report submitted. Thank you.", report: data?.[0] });
  } catch (err) {
    console.error("[API] Unexpected error submitting report:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get published books for a specific author (public endpoint)
app.get("/api/author/:authorId/books", async (req, res) => {
  try {
    const authorId = req.params.authorId;
    console.log(
      "[Author Books API] Fetching published books for author:",
      authorId
    );

    // Get all published books authored by this user
    const { data: books, error } = await supabaseAdmin
      .from("books")
      .select(
        `
        id,
        title,
        description,
        price,
        cover_url,
        status,
        created_at,
        views_count,
        rating_avg,
        book_genres(genres(name, slug)),
        profiles:author_id(username)
      `
      )
      .eq("author_id", authorId)
      .eq("status", "published")
      .order("created_at", { ascending: false });

    if (error) {
      console.error("[Author Books API] Error fetching books:", error);
      return res.status(500).json({ error: error.message });
    }

    // Get transaction and view stats for each book
    const bookIds = (books || []).map((b) => b.id).filter(Boolean);
    let statsMap = {};

    if (bookIds.length > 0) {
      const { data: txs } = await supabaseAdmin
        .from("transactions")
        .select("book_id, amount")
        .in("book_id", bookIds)
        .eq("payment_status", "completed");

      const { data: views } = await supabaseAdmin
        .from("book_views")
        .select("book_id")
        .in("book_id", bookIds);

      bookIds.forEach((id) => {
        statsMap[id] = {
          totalSalesAmount: 0,
          totalSalesCount: 0,
          totalViews: 0,
        };
      });

      (txs || []).forEach((tx) => {
        if (statsMap[tx.book_id]) {
          statsMap[tx.book_id].totalSalesAmount += Number(tx.amount || 0);
          statsMap[tx.book_id].totalSalesCount += 1;
        }
      });

      (views || []).forEach((v) => {
        if (statsMap[v.book_id]) {
          statsMap[v.book_id].totalViews += 1;
        }
      });
    }

    // Format response
    const formattedBooks = (books || []).map((b) => ({
      id: b.id,
      title: b.title,
      description: b.description,
      price: b.price || 0,
      coverUrl: b.cover_url || "",
      status: b.status || "published",
      createdAt: b.created_at,
      viewsCount: b.views_count || 0,
      ratingAvg: b.rating_avg || 0,
      category: b.book_genres?.[0]?.genres?.name || "Fiction",
      genres: b.book_genres?.map((bg) => bg.genres?.name).filter(Boolean) || [],
      author: b.profiles?.username || "Unknown Author",
      ...statsMap[b.id],
    }));

    console.log(
      "[Author Books API] Returning",
      formattedBooks.length,
      "published books"
    );
    res.json(formattedBooks);
  } catch (e) {
    console.error("Error fetching author books:", e);
    res.status(500).json({
      error: "Failed to fetch books",
      details: e.message || "Server error",
    });
  }
});

// Get all books for the authenticated author
app.get("/api/publisher/books", verifyUser, async (req, res) => {
  try {
    const userId = req.user.id;
    console.log("[Publisher Books API] Fetching books for user:", userId);

    // Get all books authored by this user with chapters and genre info
    const { data: books, error } = await supabaseAdmin
      .from("books")
      .select(
        `
        id,
        title,
        description,
        price,
        cover_url,
        status,
        created_at,
        views_count,
        book_genres(genres(name, slug))
      `
      )
      .eq("author_id", userId)
      .order("created_at", { ascending: false });

    console.log("[Publisher Books API] Query error:", error);
    console.log("[Publisher Books API] Books found:", books?.length || 0);
    console.log("[Publisher Books API] Books data:", books);

    if (error) {
      console.error("[Publisher Books API] Error fetching books:", error);
      return res.status(500).json({ error: error.message });
    }

    // Get transaction and view stats for each book
    const bookIds = (books || []).map((b) => b.id).filter(Boolean);
    let statsMap = {};

    if (bookIds.length > 0) {
      const { data: txs } = await supabaseAdmin
        .from("transactions")
        .select("book_id, amount")
        .in("book_id", bookIds)
        .eq("payment_status", "completed");

      const { data: views } = await supabaseAdmin
        .from("book_views")
        .select("book_id")
        .in("book_id", bookIds);

      bookIds.forEach((id) => {
        statsMap[id] = {
          totalSalesAmount: 0,
          totalSalesCount: 0,
          totalViews: 0,
        };
      });

      (txs || []).forEach((tx) => {
        if (statsMap[tx.book_id]) {
          statsMap[tx.book_id].totalSalesAmount += Number(tx.amount || 0);
          statsMap[tx.book_id].totalSalesCount += 1;
        }
      });

      (views || []).forEach((v) => {
        if (statsMap[v.book_id]) {
          statsMap[v.book_id].totalViews += 1;
        }
      });
    }

    // Format response
    const formattedBooks = (books || []).map((b) => ({
      id: b.id,
      title: b.title,
      description: b.description,
      price: b.price || 0,
      coverUrl: b.cover_url || "",
      status: b.status || "published",
      createdAt: b.created_at,
      viewsCount: b.views_count || 0,
      category: b.book_genres?.[0]?.genres?.name || "Fiction",
      genres: b.book_genres?.map((bg) => bg.genres?.name).filter(Boolean) || [],
      ...statsMap[b.id],
    }));

    console.log(
      "[Publisher Books API] Returning",
      formattedBooks.length,
      "books"
    );
    res.json(formattedBooks);
  } catch (e) {
    console.error("Error fetching publisher books:", e);
    res.status(500).json({
      error: "Failed to fetch books",
      details: e.message || "Server error",
    });
  }
});

// Get Publisher Analytics Data (Time-series)
app.get("/api/publisher/analytics", verifyUser, async (req, res) => {
  try {
    const userId = req.user.id;
    const { range = "7D" } = req.query;
    console.log(
      "[Analytics API] Fetching analytics for user:",
      userId,
      "Range:",
      range
    );

    // Get all books for this publisher
    const { data: myBooks, error: booksError } = await supabaseAdmin
      .from("books")
      .select("id")
      .eq("author_id", userId);

    if (booksError || !myBooks || myBooks.length === 0) {
      console.log("[Analytics API] No books found");
      return res.json([]);
    }

    const bookIds = myBooks.map((b) => b.id).filter(Boolean);

    // Calculate date range
    let daysBack = 7;
    if (range === "TODAY") daysBack = 0;
    else if (range === "7D") daysBack = 7;
    else if (range === "30D") daysBack = 30;

    const startDate = new Date();
    startDate.setDate(startDate.getDate() - daysBack);

    // Fetch transactions and views for this publisher's books within date range
    const { data: txs } = await supabaseAdmin
      .from("transactions")
      .select("created_at, amount")
      .in("book_id", bookIds)
      .eq("payment_status", "completed")
      .gte("created_at", startDate.toISOString());

    const { data: views } = await supabaseAdmin
      .from("book_views")
      .select("viewed_at")
      .in("book_id", bookIds)
      .gte("viewed_at", startDate.toISOString());

    // Aggregate data by time period
    const dataMap = {};

    // Helper to format dates
    const getKey = (dateStr, rangeType) => {
      const date = new Date(dateStr);
      if (rangeType === "TODAY") {
        const hour = date.getHours();
        const nextHour = hour + 1;
        return `${hour}:00-${nextHour}:00`;
      } else if (rangeType === "7D") {
        const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
        return days[date.getDay()];
      } else {
        return `Day ${date.getDate()}`;
      }
    };

    // Process transactions
    (txs || []).forEach((tx) => {
      const key = getKey(tx.created_at, range);
      if (!dataMap[key]) {
        dataMap[key] = { name: key, sales: 0, revenue: 0, views: 0 };
      }
      dataMap[key].sales += 1;
      dataMap[key].revenue += Number(tx.amount || 0);
    });

    // Process views
    (views || []).forEach((view) => {
      const key = getKey(view.viewed_at, range);
      if (!dataMap[key]) {
        dataMap[key] = { name: key, sales: 0, revenue: 0, views: 0 };
      }
      dataMap[key].views += 1;
    });

    // Generate labels for missing periods
    const labels = [];
    if (range === "TODAY") {
      for (let i = 0; i < 24; i++) {
        const nextHour = i + 1;
        const label = `${i}:00-${nextHour}:00`;
        labels.push(label);
      }
    } else if (range === "7D") {
      const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
      labels.push(...days);
    } else {
      for (let i = 1; i <= 31; i++) {
        labels.push(`Day ${i}`);
      }
    }

    // Build final data array with all periods
    const result = labels.map(
      (label) =>
        dataMap[label] || { name: label, sales: 0, revenue: 0, views: 0 }
    );

    console.log(
      "[Analytics API] Returning analytics data with",
      result.length,
      "periods"
    );
    res.json(result);
  } catch (e) {
    console.error("Error fetching analytics:", e);
    res.status(500).json({
      error: "Failed to fetch analytics",
      details: e.message || "Server error",
    });
  }
});

// ==========================================
//       AUTHOR WITHDRAWAL / PAYOUT
// ==========================================

// Request Withdrawal (Author initiates payout)
app.post("/api/author/withdraw", verifyUser, async (req, res) => {
  const { amount } = req.body;
  const authorId = req.user.id;

  // Validate input
  if (!amount || amount <= 0) {
    return res.status(400).json({ error: "Invalid withdrawal amount" });
  }

  // 1. Fetch platform settings for payout fee
  let payoutFeePercent = 2; // default 2% if not found
  const { data: platformSettings } = await supabase
    .from("platform_settings")
    .select("payout_fee_percent")
    .eq("id", "default")
    .single();

  if (platformSettings && platformSettings.payout_fee_percent !== undefined) {
    payoutFeePercent = platformSettings.payout_fee_percent;
    console.log(
      `[WITHDRAWAL] Using payout fee percentage from DB: ${payoutFeePercent}%`
    );
  } else {
    console.log(
      `[WITHDRAWAL] Platform settings not found, using default fee: ${payoutFeePercent}%`
    );
  }

  // 2. Calculate admin fee and author net amount
  const adminFee = parseFloat((amount * (payoutFeePercent / 100)).toFixed(2));
  const authorNet = parseFloat((amount - adminFee).toFixed(2));

  // 3. Check author's available balance (sum of author earnings from SALE transactions)
  const { data: saleTransactions, error: saleError } = await supabase
    .from("transactions")
    .select("author_earning")
    .eq("author_id", authorId)
    .eq("type", "SALE")
    .eq("status", "COMPLETED");

  if (saleError) {
    console.error("Error fetching author balance:", saleError);
    return res.status(500).json({ error: "Failed to check balance" });
  }

  const totalAvailableBalance = (saleTransactions || []).reduce(
    (sum, txn) => sum + (txn.author_earning || 0),
    0
  );

  // 4. Check if author has sufficient balance (including pending payout amounts)
  const { data: pendingPayouts, error: pendingError } = await supabase
    .from("transactions")
    .select("amount")
    .eq("author_id", authorId)
    .eq("type", "PAYOUT")
    .eq("status", "PENDING");

  if (pendingError) {
    console.error("Error fetching pending payouts:", pendingError);
    return res.status(500).json({ error: "Failed to check pending payouts" });
  }

  const totalPendingPayouts = (pendingPayouts || []).reduce(
    (sum, txn) => sum + (txn.amount || 0),
    0
  );

  const availableBalance = totalAvailableBalance - totalPendingPayouts;

  if (availableBalance < amount) {
    return res.status(400).json({
      error: `Insufficient balance. Available: $${availableBalance.toFixed(
        2
      )}, Requested: $${amount.toFixed(2)}`,
      available: availableBalance,
    });
  }

  // 5. Create PAYOUT transaction record
  const payoutPayload = {
    author_id: authorId,
    type: "PAYOUT",
    status: "PENDING",
    amount: amount,
    admin_fee: adminFee,
    author_earning: authorNet,
    payment_status: "pending",
    created_at: new Date().toISOString(),
  };

  console.log(
    "[WITHDRAWAL] Inserting payout with payload:",
    JSON.stringify(payoutPayload)
  );

  const { data: payoutTx, error: payoutError } = await supabaseAdmin
    .from("transactions")
    .insert(payoutPayload)
    .select()
    .single();

  if (payoutError) {
    console.error("[WITHDRAWAL] Payout transaction insert error:", payoutError);
    console.error("[WITHDRAWAL] Error code:", payoutError.code);
    console.error("[WITHDRAWAL] Error hint:", payoutError.hint);
    return res.status(500).json({
      error: "Failed to create withdrawal request",
      details: payoutError.message,
    });
  }

  if (!payoutTx) {
    console.error("[WITHDRAWAL] No payout transaction data returned");
    return res.status(500).json({
      error: "Failed to create withdrawal request - no data returned",
    });
  }

  console.log(
    "[WITHDRAWAL] Payout transaction created successfully:",
    JSON.stringify(payoutTx)
  );

  // 6. Create notification for author
  await supabaseAdmin.from("notifications").insert({
    user_id: authorId,
    type: "withdrawal_pending",
    title: "Withdrawal Request Received",
    message: `Your withdrawal of $${amount.toFixed(
      2
    )} is pending. Fee: $${adminFee.toFixed(
      2
    )}. You'll receive: $${authorNet.toFixed(2)}`,
    link_url: "/dashboard/earnings",
  });

  console.log(
    `[WITHDRAWAL] New payout created - Author: ${authorId}, Amount: $${amount.toFixed(
      2
    )}, Fee (${payoutFeePercent}%): $${adminFee.toFixed(
      2
    )}, Net: $${authorNet.toFixed(2)}`
  );

  res.json({
    success: true,
    transactionId: payoutTx.id,
    requestedAmount: amount,
    adminFee: adminFee,
    netAmount: authorNet,
    status: "PENDING",
  });
});

// Start Server
const options = {
  key: fs.readFileSync(
    "/etc/letsencrypt/live/srv1202611.hstgr.cloud/privkey.pem"
  ),
  cert: fs.readFileSync(
    "/etc/letsencrypt/live/srv1202611.hstgr.cloud/fullchain.pem"
  ),
};

https.createServer(options, app).listen(port, () => {
  console.log(
    `Lumina Server running on https://srv1202611.hstgr.cloud:${port}`
  );
});
